package com.example.ingestion.batch.reader

import com.example.ingestion.dto.*
import com.example.ingestion.entity.JobExecutionState
import com.example.ingestion.repository.JobExecutionStateRepository
import com.fasterxml.jackson.databind.ObjectMapper
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Timer
import org.slf4j.LoggerFactory
import org.slf4j.MDC
import org.springframework.batch.item.ItemReader
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.beans.factory.annotation.Value
import org.springframework.http.HttpHeaders
import org.springframework.retry.annotation.Backoff
import org.springframework.retry.annotation.Retryable
import org.springframework.stereotype.Component
import org.springframework.web.reactive.function.client.WebClient
import org.springframework.web.reactive.function.client.WebClientResponseException
import reactor.core.publisher.Mono
import reactor.util.retry.Retry
import java.math.BigDecimal
import java.time.Duration
import java.time.LocalDateTime
import java.time.OffsetDateTime
import java.util.concurrent.atomic.AtomicInteger

@Component
class RegionalNaverGooglePlaceReader(
    @Qualifier("externalApiWebClient") private val externalApiWebClient: WebClient,
    private val jobExecutionStateRepository: JobExecutionStateRepository,
    private val meterRegistry: MeterRegistry,
    private val objectMapper: ObjectMapper,
    @Value("\${app.external.naver.base-url}") private val naverBaseUrl: String,
    @Value("\${app.external.naver.client-id}") private val naverClientId: String,
    @Value("\${app.external.naver.client-secret}") private val naverClientSecret: String,
    @Value("\${app.external.naver.page-size:10}") private val naverPageSize: Int,
    @Value("\${app.external.naver.max-pages:50}") private val naverMaxPages: Int,
    @Value("\${app.external.naver.timeout:10}") private val naverTimeout: Int,
    @Value("\${app.external.google.base-url}") private val googleBaseUrl: String,
    @Value("\${app.external.google.api-key}") private val googleApiKey: String,
    @Value("\${app.external.google.timeout:15}") private val googleTimeout: Int,
    @Value("\${app.external.google.search-radius:100}") private val googleSearchRadius: Int,
    @Value("\${app.external.google.photo-max-width:400}") private val googlePhotoMaxWidth: Int,
    @Value("\${app.batch.job-name:regional-place-ingestion}") private val jobName: String
) : ItemReader<EnrichedPlace> {

    private val logger = LoggerFactory.getLogger(RegionalNaverGooglePlaceReader::class.java)
    private val naverApiTimer = Timer.builder("regional_naver_api_calls").register(meterRegistry)
    private val googleApiTimer = Timer.builder("regional_google_api_calls").register(meterRegistry)

    private val regions = KoreanRegion.values().sortedBy { it.priority }
    
    private var currentBatch = mutableListOf<EnrichedPlace>()
    private var currentIndex = 0
    private var processingState = RegionalProcessingState(regions.first())
    private var hasMoreData = true
    private var initialized = false
    
    companion object {
        private const val CORRELATION_ID = "correlationId"
    }

    override fun read(): EnrichedPlace? {
        if (!initialized) {
            initialize()
            initialized = true
        }

        // Return items from current batch
        if (currentIndex < currentBatch.size) {
            return currentBatch[currentIndex++]
        }

        // Check if we're done with all regions
        if (!hasMoreData) {
            logger.info("Completed reading all Korean regional place data. Processed ${processingState.totalProcessedPlaces} places across ${processingState.completedRegions.size} regions")
            return null
        }

        // Fetch next batch
        fetchNextBatch()?.let { places ->
            currentBatch = places.toMutableList()
            currentIndex = 0
            return if (currentBatch.isNotEmpty()) {
                currentIndex++
                currentBatch[0]
            } else null
        }

        return null
    }

    private fun initialize() {
        val correlationId = generateCorrelationId()
        MDC.put(CORRELATION_ID, correlationId)
        
        logger.info("Initializing RegionalNaverGooglePlaceReader for comprehensive Korean coverage")
        logger.info("Processing order: ${regions.joinToString(" → ") { it.regionName }}")
        
        // Try to resume from last processed state
        val lastState = jobExecutionStateRepository.findByJobName(jobName)
        if (lastState.isPresent) {
            val state = lastState.get()
            logger.info("Resuming from previous state: ${state.lastExecutionStatus}")
            // Resume logic can be implemented here if needed
        } else {
            logger.info("Starting fresh regional processing from ${regions.first().regionName}")
        }
        
        meterRegistry.gauge("regional_batch_current_region", processingState.currentRegion.priority)
    }

    private fun fetchNextBatch(): List<EnrichedPlace>? {
        val currentRegion = processingState.currentRegion
        
        // Check if current region is complete
        if (isCurrentRegionComplete()) {
            if (!moveToNextRegion()) {
                hasMoreData = false
                return null
            }
        }

        val context = getCurrentSearchContext()
        logger.info("Processing ${currentRegion.regionName}: ${context.coordinate.description} - '${context.query}' (page ${context.page})")

        return try {
            val enrichedPlaces = mutableListOf<EnrichedPlace>()
            
            // Fetch from Naver API
            val naverResponse = fetchNaverPlaces(context)
            if (naverResponse.items.isEmpty()) {
                moveToNextQuery()
                return emptyList()
            }

            logger.debug("Fetched ${naverResponse.items.size} places from Naver for ${currentRegion.regionName}")

            // Enrich each Naver place with Google data
            for (naverPlace in naverResponse.items) {
                try {
                    val googlePlace = enrichWithGoogle(naverPlace)
                    val photoUrl = googlePlace?.photos?.firstOrNull()?.let { photo ->
                        buildGooglePhotoUrl(photo.photoReference)
                    }
                    
                    enrichedPlaces.add(EnrichedPlace(
                        naverPlace = naverPlace,
                        googlePlace = googlePlace,
                        googlePhotoUrl = photoUrl,
                        searchContext = PlaceSearchContext(context.query, 
                            SeoulCoordinate(context.coordinate.lat, context.coordinate.lng, context.coordinate.radius), 
                            context.page)
                    ))

                    meterRegistry.counter("regional_places_enriched", "region", currentRegion.regionName).increment()
                } catch (e: Exception) {
                    logger.warn("Failed to enrich place ${naverPlace.cleanTitle} in ${currentRegion.regionName}: ${e.message}")
                    meterRegistry.counter("regional_places_enrichment_failed", "region", currentRegion.regionName).increment()
                    
                    // Still add the Naver-only place
                    enrichedPlaces.add(EnrichedPlace(
                        naverPlace = naverPlace,
                        googlePlace = null,
                        googlePhotoUrl = null,
                        searchContext = PlaceSearchContext(context.query, 
                            SeoulCoordinate(context.coordinate.lat, context.coordinate.lng, context.coordinate.radius), 
                            context.page)
                    ))
                }
            }

            // Move to next page/query/coordinate
            moveToNextPage()
            updateJobState()
            
            processingState = processingState.copy(
                totalProcessedPlaces = processingState.totalProcessedPlaces + enrichedPlaces.size
            )
            
            logger.info("Processed batch for ${currentRegion.regionName}: ${enrichedPlaces.size} places")
            enrichedPlaces
            
        } catch (ex: Exception) {
            logger.error("Error fetching batch for ${currentRegion.regionName}: ${ex.message}", ex)
            meterRegistry.counter("regional_batch_fetch_errors", "region", currentRegion.regionName).increment()
            processingState.errors.add("${currentRegion.regionName}: ${ex.message}")
            moveToNextQuery() // Skip problematic query
            emptyList()
        }
    }

    private fun isCurrentRegionComplete(): Boolean {
        val region = processingState.currentRegion
        return processingState.currentCoordinateIndex >= region.coordinates.size
    }

    private fun moveToNextRegion(): Boolean {
        val currentRegionIndex = regions.indexOf(processingState.currentRegion)
        if (currentRegionIndex >= regions.size - 1) {
            return false // No more regions
        }
        
        val completedRegions = processingState.completedRegions + processingState.currentRegion
        val nextRegion = regions[currentRegionIndex + 1]
        
        logger.info("Completed region: ${processingState.currentRegion.regionName}. Moving to: ${nextRegion.regionName}")
        
        processingState = RegionalProcessingState(
            currentRegion = nextRegion,
            completedRegions = completedRegions,
            currentCoordinateIndex = 0,
            currentQueryIndex = 0,
            currentPage = 1,
            totalProcessedPlaces = processingState.totalProcessedPlaces,
            errors = processingState.errors
        )
        
        meterRegistry.gauge("regional_batch_current_region", nextRegion.priority)
        return true
    }

    private fun getCurrentSearchContext(): RegionalSearchContext {
        val region = processingState.currentRegion
        val coordinate = region.coordinates[processingState.currentCoordinateIndex]
        val query = region.searchQueries[processingState.currentQueryIndex]
        
        return RegionalSearchContext(
            region = region,
            coordinate = coordinate,
            query = query,
            page = processingState.currentPage
        )
    }

    private fun moveToNextPage() {
        processingState = processingState.copy(currentPage = processingState.currentPage + 1)
        
        // If we've reached max pages, move to next query
        if (processingState.currentPage > naverMaxPages) {
            moveToNextQuery()
        }
    }

    private fun moveToNextQuery() {
        val region = processingState.currentRegion
        val nextQueryIndex = processingState.currentQueryIndex + 1
        
        if (nextQueryIndex >= region.searchQueries.size) {
            // Move to next coordinate
            val nextCoordinateIndex = processingState.currentCoordinateIndex + 1
            processingState = processingState.copy(
                currentCoordinateIndex = nextCoordinateIndex,
                currentQueryIndex = 0,
                currentPage = 1
            )
        } else {
            processingState = processingState.copy(
                currentQueryIndex = nextQueryIndex,
                currentPage = 1
            )
        }
    }

    @Retryable(
        value = [WebClientResponseException::class],
        maxAttempts = 3,
        backoff = Backoff(delay = 1000, multiplier = 2.0, maxDelay = 10000)
    )
    private fun fetchNaverPlaces(context: RegionalSearchContext): NaverLocalSearchResponse {
        return naverApiTimer.recordCallable {
            val locationQuery = "${context.query} ${context.coordinate.description}"
            
            externalApiWebClient.get()
                .uri { builder ->
                    builder
                        .scheme("https")
                        .host("openapi.naver.com")
                        .path("/v1/search/local.json")
                        .queryParam("query", locationQuery)
                        .queryParam("display", naverPageSize)
                        .queryParam("start", (context.page - 1) * naverPageSize + 1)
                        .queryParam("sort", "random")
                        .build()
                }
                .header("X-Naver-Client-Id", naverClientId)
                .header("X-Naver-Client-Secret", naverClientSecret)
                .retrieve()
                .bodyToMono(NaverLocalSearchResponse::class.java)
                .retryWhen(
                    Retry.backoff(2, Duration.ofSeconds(1))
                        .maxBackoff(Duration.ofSeconds(10))
                        .filter { it is WebClientResponseException && it.statusCode.is5xxServerError }
                )
                .doOnSuccess {
                    meterRegistry.counter("regional_naver_api_success", "region", context.region.regionName).increment()
                }
                .doOnError { error ->
                    logger.error("Naver API error for ${context.region.regionName} - ${context.query}: ${error.message}")
                    meterRegistry.counter("regional_naver_api_error", "region", context.region.regionName).increment()
                }
                .block(Duration.ofSeconds(naverTimeout.toLong())) ?: NaverLocalSearchResponse(0, 0, 0, emptyList())
        }!!
    }

    @Retryable(
        value = [WebClientResponseException::class],
        maxAttempts = 3,
        backoff = Backoff(delay = 1000, multiplier = 2.0, maxDelay = 10000)
    )
    private fun enrichWithGoogle(naverPlace: NaverPlaceItem): GooglePlaceDetail? {
        return googleApiTimer.recordCallable {
            try {
                // First, try nearby search
                val nearbyResponse = externalApiWebClient.get()
                    .uri { builder ->
                        builder
                            .scheme("https")
                            .host("maps.googleapis.com")
                            .path("/maps/api/place/nearbysearch/json")
                            .queryParam("location", "${naverPlace.latitude},${naverPlace.longitude}")
                            .queryParam("radius", googleSearchRadius)
                            .queryParam("keyword", naverPlace.cleanTitle)
                            .queryParam("key", googleApiKey)
                            .queryParam("language", "ko")
                            .build()
                    }
                    .retrieve()
                    .bodyToMono(GoogleNearbySearchResponse::class.java)
                    .retryWhen(
                        Retry.backoff(2, Duration.ofSeconds(1))
                            .maxBackoff(Duration.ofSeconds(5))
                            .filter { it is WebClientResponseException && it.statusCode.is5xxServerError }
                    )
                    .block(Duration.ofSeconds(googleTimeout.toLong()))

                var googlePlaceId: String? = nearbyResponse?.results?.firstOrNull()?.placeId

                // If nearby search didn't find anything, try text search
                if (googlePlaceId == null) {
                    val textResponse = externalApiWebClient.get()
                        .uri { builder ->
                            builder
                                .scheme("https")
                                .host("maps.googleapis.com")
                                .path("/maps/api/place/textsearch/json")
                                .queryParam("query", "${naverPlace.cleanTitle} ${naverPlace.address}")
                                .queryParam("key", googleApiKey)
                                .queryParam("language", "ko")
                                .build()
                        }
                        .retrieve()
                        .bodyToMono(GoogleTextSearchResponse::class.java)
                        .retryWhen(
                            Retry.backoff(2, Duration.ofSeconds(1))
                                .maxBackoff(Duration.ofSeconds(5))
                                .filter { it is WebClientResponseException && it.statusCode.is5xxServerError }
                        )
                        .block(Duration.ofSeconds(googleTimeout.toLong()))
                        
                    googlePlaceId = textResponse?.results?.firstOrNull()?.placeId
                }

                // If we found a place, get detailed information
                googlePlaceId?.let { placeId ->
                    Thread.sleep(50) // Rate limiting between Google API calls
                    
                    val detailsResponse = externalApiWebClient.get()
                        .uri { builder ->
                            builder
                                .scheme("https")
                                .host("maps.googleapis.com")
                                .path("/maps/api/place/details/json")
                                .queryParam("place_id", placeId)
                                .queryParam("fields", "place_id,name,formatted_address,formatted_phone_number,website,opening_hours,rating,user_ratings_total,price_level,types,photos,geometry,reviews")
                                .queryParam("key", googleApiKey)
                                .queryParam("language", "ko")
                                .build()
                        }
                        .retrieve()
                        .bodyToMono(GooglePlaceDetailsResponse::class.java)
                        .retryWhen(
                            Retry.backoff(2, Duration.ofSeconds(1))
                                .maxBackoff(Duration.ofSeconds(5))
                                .filter { it is WebClientResponseException && it.statusCode.is5xxServerError }
                        )
                        .block(Duration.ofSeconds(googleTimeout.toLong()))

                    meterRegistry.counter("regional_google_api_success").increment()
                    detailsResponse?.result
                } ?: run {
                    meterRegistry.counter("regional_google_place_not_found").increment()
                    null
                }
            } catch (e: Exception) {
                logger.warn("Google API enrichment failed for ${naverPlace.cleanTitle}: ${e.message}")
                meterRegistry.counter("regional_google_api_error").increment()
                null
            }
        }
    }

    private fun buildGooglePhotoUrl(photoReference: String): String {
        return "https://maps.googleapis.com/maps/api/place/photo?maxwidth=$googlePhotoMaxWidth&photo_reference=$photoReference&key=$googleApiKey"
    }

    private fun updateJobState() {
        try {
            val currentRegion = processingState.currentRegion
            val progress = "${currentRegion.regionName}:${processingState.currentCoordinateIndex}:${processingState.currentQueryIndex}:${processingState.currentPage}"
            
            jobExecutionStateRepository.upsertByJobName(
                jobName = jobName,
                lastProcessedPage = processingState.currentRegion.priority,
                lastProcessedTimestamp = OffsetDateTime.now(),
                totalProcessedRecords = processingState.totalProcessedPlaces,
                lastExecutionStatus = "PROCESSING_${currentRegion.regionName}",
                createdAt = LocalDateTime.now(),
                updatedAt = LocalDateTime.now()
            )
        } catch (ex: Exception) {
            logger.warn("Failed to update job state: ${ex.message}")
        }
    }

    private fun generateCorrelationId(): String {
        return "regional-batch-${System.currentTimeMillis()}-${Thread.currentThread().id}"
    }
}