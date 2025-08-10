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

data class SeoulCoordinate(
    val lat: BigDecimal,
    val lng: BigDecimal, 
    val radius: Int
)

data class PlaceSearchContext(
    val query: String,
    val coordinate: SeoulCoordinate,
    val page: Int
)

data class EnrichedPlace(
    val naverPlace: NaverPlaceItem,
    val googlePlace: GooglePlaceDetail?,
    val googlePhotoUrl: String?,
    val searchContext: PlaceSearchContext
)

@Component
class NaverGooglePlaceReader(
    private val webClient: WebClient,
    private val jobExecutionStateRepository: JobExecutionStateRepository,
    private val meterRegistry: MeterRegistry,
    private val objectMapper: ObjectMapper,
    @Value("\${app.external.naver.base-url}") private val naverBaseUrl: String,
    @Value("\${app.external.naver.client-id}") private val naverClientId: String,
    @Value("\${app.external.naver.client-secret}") private val naverClientSecret: String,
    @Value("\${app.external.naver.page-size:5}") private val naverPageSize: Int,
    @Value("\${app.external.naver.max-pages:200}") private val naverMaxPages: Int,
    @Value("\${app.external.naver.timeout:10}") private val naverTimeout: Int,
    @Value("\${app.external.google.base-url}") private val googleBaseUrl: String,
    @Value("\${app.external.google.api-key}") private val googleApiKey: String,
    @Value("\${app.external.google.timeout:15}") private val googleTimeout: Int,
    @Value("\${app.external.google.search-radius:100}") private val googleSearchRadius: Int,
    @Value("\${app.external.google.photo-max-width:400}") private val googlePhotoMaxWidth: Int,
    @Value("\${app.batch.job-name:naver-google-place-ingestion}") private val jobName: String
) : ItemReader<EnrichedPlace> {

    private val logger = LoggerFactory.getLogger(NaverGooglePlaceReader::class.java)
    private val naverApiTimer = Timer.builder("naver_api_calls").register(meterRegistry)
    private val googleApiTimer = Timer.builder("google_api_calls").register(meterRegistry)

    // Seoul search configuration
    private val queries = listOf("카페", "레스토랑", "음식점", "펍", "바", "베이커리", "디저트", "공원", "박물관", "미술관", "서점", "쇼핑몰", "영화관", "헬스장", "스파")
    private val seoulCoords = listOf(
        SeoulCoordinate(BigDecimal("37.5665"), BigDecimal("126.9780"), 5000), // 중구
        SeoulCoordinate(BigDecimal("37.5172"), BigDecimal("127.0473"), 5000), // 강남구
        SeoulCoordinate(BigDecimal("37.5440"), BigDecimal("127.0557"), 5000), // 성동구
        SeoulCoordinate(BigDecimal("37.5219"), BigDecimal("126.9895"), 5000), // 용산구
        SeoulCoordinate(BigDecimal("37.5636"), BigDecimal("126.9748"), 5000), // 중구
        SeoulCoordinate(BigDecimal("37.5502"), BigDecimal("126.9224"), 5000), // 마포구
        SeoulCoordinate(BigDecimal("37.5465"), BigDecimal("127.0949"), 5000), // 광진구
        SeoulCoordinate(BigDecimal("37.5814"), BigDecimal("127.0097"), 5000)  // 종로구
    )

    private var currentBatch = mutableListOf<EnrichedPlace>()
    private var currentIndex = 0
    private var searchContextIndex = 0
    private var currentPage = AtomicInteger(1)
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

        // Check if we're done with all search contexts
        if (!hasMoreData) {
            logger.info("Completed reading all Seoul place data. Total contexts processed: $searchContextIndex")
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
        
        logger.info("Initializing NaverGooglePlaceReader for Seoul coverage with ${queries.size} queries x ${seoulCoords.size} coordinates")
        
        // Try to resume from last processed state
        val lastState = jobExecutionStateRepository.findByJobName(jobName)
        if (lastState.isPresent) {
            searchContextIndex = lastState.get().lastProcessedPage
            logger.info("Resuming from search context index $searchContextIndex")
        } else {
            logger.info("Starting fresh from search context 0")
        }
        
        meterRegistry.gauge("batch_current_context", searchContextIndex)
    }

    private fun fetchNextBatch(): List<EnrichedPlace>? {
        if (searchContextIndex >= getTotalSearchContexts()) {
            hasMoreData = false
            return null
        }

        val context = getSearchContext(searchContextIndex)
        logger.info("Processing search context ${searchContextIndex + 1}/${getTotalSearchContexts()}: query='${context.query}', coord=${context.coordinate.lat},${context.coordinate.lng}")

        return try {
            val enrichedPlaces = mutableListOf<EnrichedPlace>()
            var currentNaverPage = 1

            // Paginate through Naver results
            while (currentNaverPage <= naverMaxPages) {
                val naverResponse = fetchNaverPlaces(context.copy(page = currentNaverPage))
                if (naverResponse.items.isEmpty()) break

                logger.debug("Fetched ${naverResponse.items.size} places from Naver (page $currentNaverPage)")

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
                            searchContext = context
                        ))

                        meterRegistry.counter("places_enriched").increment()
                    } catch (e: Exception) {
                        logger.warn("Failed to enrich place ${naverPlace.cleanTitle}: ${e.message}")
                        meterRegistry.counter("places_enrichment_failed").increment()
                        
                        // Still add the Naver-only place
                        enrichedPlaces.add(EnrichedPlace(
                            naverPlace = naverPlace,
                            googlePlace = null,
                            googlePhotoUrl = null,
                            searchContext = context
                        ))
                    }
                }

                currentNaverPage++
                
                // Rate limiting
                Thread.sleep(100) // 100ms between Naver API calls
            }

            // Move to next search context
            searchContextIndex++
            updateJobState()
            
            logger.info("Completed search context, enriched ${enrichedPlaces.size} places")
            enrichedPlaces
            
        } catch (ex: Exception) {
            logger.error("Error fetching batch for context $searchContextIndex: ${ex.message}", ex)
            meterRegistry.counter("batch_fetch_errors").increment()
            searchContextIndex++ // Skip problematic context
            emptyList()
        }
    }

    @Retryable(
        value = [WebClientResponseException::class],
        maxAttempts = 3,
        backoff = Backoff(delay = 1000, multiplier = 2.0, maxDelay = 10000)
    )
    private fun fetchNaverPlaces(context: PlaceSearchContext): NaverLocalSearchResponse {
        return naverApiTimer.recordCallable {
            webClient.get()
                .uri { builder ->
                    builder
                        .path(naverBaseUrl)
                        .queryParam("query", context.query)
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
                    meterRegistry.counter("naver_api_success").increment()
                }
                .doOnError { error ->
                    logger.error("Naver API error for ${context.query}: ${error.message}")
                    meterRegistry.counter("naver_api_error").increment()
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
                val nearbyResponse = webClient.get()
                    .uri { builder ->
                        builder
                            .path("$googleBaseUrl/nearbysearch/json")
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
                    val textResponse = webClient.get()
                        .uri { builder ->
                            builder
                                .path("$googleBaseUrl/textsearch/json")
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
                    
                    val detailsResponse = webClient.get()
                        .uri { builder ->
                            builder
                                .path("$googleBaseUrl/details/json")
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

                    meterRegistry.counter("google_api_success").increment()
                    detailsResponse?.result
                } ?: run {
                    meterRegistry.counter("google_place_not_found").increment()
                    null
                }
            } catch (e: Exception) {
                logger.warn("Google API enrichment failed for ${naverPlace.cleanTitle}: ${e.message}")
                meterRegistry.counter("google_api_error").increment()
                null
            }
        }
    }

    private fun buildGooglePhotoUrl(photoReference: String): String {
        return "$googleBaseUrl/photo?maxwidth=$googlePhotoMaxWidth&photo_reference=$photoReference&key=$googleApiKey"
    }

    private fun getSearchContext(index: Int): PlaceSearchContext {
        val totalCoordsPerQuery = seoulCoords.size
        val queryIndex = index / totalCoordsPerQuery
        val coordIndex = index % totalCoordsPerQuery
        
        return PlaceSearchContext(
            query = queries[queryIndex],
            coordinate = seoulCoords[coordIndex],
            page = 1 // This will be overridden during pagination
        )
    }

    private fun getTotalSearchContexts(): Int = queries.size * seoulCoords.size

    private fun updateJobState() {
        try {
            jobExecutionStateRepository.upsertByJobName(
                jobName = jobName,
                lastProcessedPage = searchContextIndex,
                lastProcessedTimestamp = OffsetDateTime.now(),
                totalProcessedRecords = (searchContextIndex * naverPageSize * naverMaxPages).toLong(),
                lastExecutionStatus = "RUNNING",
                createdAt = LocalDateTime.now(),
                updatedAt = LocalDateTime.now()
            )
        } catch (ex: Exception) {
            logger.warn("Failed to update job state: ${ex.message}")
        }
    }

    private fun generateCorrelationId(): String {
        return "naver-google-batch-${System.currentTimeMillis()}-${Thread.currentThread().id}"
    }
}