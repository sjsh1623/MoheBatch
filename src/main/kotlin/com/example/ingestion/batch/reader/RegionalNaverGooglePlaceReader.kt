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
    
    // Thread-safe state that resets for each job execution
    @Volatile
    private var currentBatch = mutableListOf<EnrichedPlace>()
    @Volatile
    private var currentIndex = 0
    @Volatile
    private var processingState = RegionalProcessingState(regions.first())
    @Volatile
    private var hasMoreData = true
    @Volatile
    private var initialized = false
    @Volatile
    private var readCount = 0
    
    companion object {
        private const val CORRELATION_ID = "correlationId"
    }

    override fun read(): EnrichedPlace? {
        // Always reset state at the beginning of each job execution
        if (readCount == 0) {
            resetReaderState()
        }
        
        readCount++
        logger.error("📍 CONTINUOUS READER - CALL #$readCount (Job execution: fresh state)")
        
        if (!initialized) {
            initialize()
            initialized = true
        }

        // Make continuous API calls - increased to collect more place data
        if (readCount <= 50) {  // Increased to 50 for more comprehensive data collection
            logger.error("📍 MAKING NAVER API CALL #$readCount")
            return makeRealApiCall(readCount)
        }
        
        logger.error("📍 COMPLETED 50 API CALLS - JOB FINISHED, NEXT SCHEDULED EXECUTION WILL START FRESH")
        return null
    }
    
    private fun resetReaderState() {
        logger.error("🔄 RESETTING READER STATE FOR NEW JOB EXECUTION")
        currentBatch.clear()
        currentIndex = 0
        processingState = RegionalProcessingState(regions.first())
        hasMoreData = true
        initialized = false
        // readCount stays as-is since we're in the middle of incrementing it
        logger.error("🔄 READER STATE RESET COMPLETE - READY FOR FRESH DATA COLLECTION")
    }
    
    private fun makeRealApiCall(callNumber: Int): EnrichedPlace? {
        return try {
            logger.error("📍 CALLING NAVER API - Call #$callNumber")
            
            // Use different coordinates and queries to get varied data
            val seoulCoords = KoreanRegion.SEOUL.coordinates
            val queries = listOf(
                "카페", "레스토랑", "음식점", "베이커리", "디저트", "펜션", "관광지", "박물관", "공원", "서점",
                "한식당", "중식당", "일식당", "양식당", "치킨집", "피자", "햄버거", "분식", "족발", "곱창",
                "호텔", "모텔", "게스트하우스", "리조트", "펜션", "민박", "캠핑장",
                "쇼핑몰", "마트", "백화점", "편의점", "아울렛", "시장", "상점",
                "영화관", "pc방", "노래방", "볼링장", "당구장", "스포츠센터", "헬스장", "수영장",
                "병원", "약국", "은행", "우체국", "주유소", "세차장", "정비소"
            )
            
            val coordIndex = (callNumber - 1) % seoulCoords.size
            val queryIndex = (callNumber - 1) % queries.size
            val page = ((callNumber - 1) / (seoulCoords.size * queries.size)) + 1
            
            val context = RegionalSearchContext(
                region = KoreanRegion.SEOUL,
                coordinate = seoulCoords[coordIndex],
                query = queries[queryIndex],
                page = page
            )
            
            logger.error("📍 API Call: ${context.query} in ${context.coordinate.description} (page $page)")
            
            val naverResponse = fetchNaverPlaces(context)
            logger.error("📍 NAVER API RETURNED ${naverResponse.items.size} places")
            
            if (naverResponse.items.isNotEmpty()) {
                // Use different place from results each time
                val placeIndex = (callNumber - 1) % naverResponse.items.size
                val selectedPlace = naverResponse.items[placeIndex]
                logger.error("📍 SELECTED PLACE: ${selectedPlace.cleanTitle}")
                
                return EnrichedPlace(
                    naverPlace = selectedPlace,
                    googlePlace = null,
                    googlePhotoUrl = null,
                    searchContext = PlaceSearchContext(
                        context.query,
                        SeoulCoordinate(
                            context.coordinate.lat,
                            context.coordinate.lng,
                            context.coordinate.radius
                        ),
                        context.page
                    )
                )
            } else {
                logger.error("📍 NO PLACES FROM NAVER - trying next call")
                return null
            }
        } catch (e: Exception) {
            logger.error("📍 API ERROR: ${e.message}", e)
            null
        }
    }

    private fun initialize() {
        val correlationId = generateCorrelationId()
        MDC.put(CORRELATION_ID, correlationId)
        
        logger.info("Initializing RegionalNaverGooglePlaceReader for comprehensive Korean coverage")
        logger.info("Processing order: ${regions.joinToString(" → ") { it.regionName }}")
        
        // Force fresh start for debugging
        logger.info("FORCE RESTARTING: Starting fresh regional processing from ${regions.first().regionName}")
        processingState = RegionalProcessingState(regions.first())
        hasMoreData = true
        
        // Debug current state
        val firstRegion = regions.first()
        logger.info("DEBUG: First region = ${firstRegion.regionName}, coordinates count = ${firstRegion.coordinates.size}, queries count = ${firstRegion.searchQueries.size}")
        logger.info("DEBUG: Initial state - coordinateIndex = ${processingState.currentCoordinateIndex}, queryIndex = ${processingState.currentQueryIndex}, page = ${processingState.currentPage}")
        logger.info("DEBUG: isCurrentRegionComplete() = ${processingState.currentCoordinateIndex >= firstRegion.coordinates.size}")
        
        meterRegistry.gauge("regional_batch_current_region", processingState.currentRegion.priority)
    }

    private fun fetchNextBatch(): List<EnrichedPlace>? {
        val currentRegion = processingState.currentRegion
        
        logger.info("DEBUG: fetchNextBatch() called - region = ${currentRegion.regionName}, coordinateIndex = ${processingState.currentCoordinateIndex}/${currentRegion.coordinates.size}")
        
        // Check if current region is complete
        if (isCurrentRegionComplete()) {
            logger.info("DEBUG: Current region ${currentRegion.regionName} is complete, trying to move to next region")
            if (!moveToNextRegion()) {
                logger.info("DEBUG: No more regions available, setting hasMoreData = false")
                hasMoreData = false
                return null
            }
        }

        val context = getCurrentSearchContext()
        logger.info("Processing ${currentRegion.regionName}: ${context.coordinate.description} - '${context.query}' (page ${context.page})")

        return try {
            val enrichedPlaces = mutableListOf<EnrichedPlace>()
            
            // Fetch from Naver API
            logger.warn("*** CALLING NAVER API *** region=${currentRegion.regionName}, query=${context.query}")
            val naverResponse = fetchNaverPlaces(context)
            logger.warn("*** NAVER API RETURNED *** ${naverResponse.items.size} items")
            
            if (naverResponse.items.isEmpty()) {
                logger.warn("*** NO ITEMS FROM NAVER *** moving to next query")
                moveToNextQuery()
                return emptyList()
            }

            logger.debug("Fetched ${naverResponse.items.size} places from Naver for ${currentRegion.regionName}, using top ${minOf(50, naverResponse.items.size)}")

            // Enrich each Naver place with Google data (top 50 most popular only)
            for (naverPlace in naverResponse.items.take(50)) {
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
            logger.error("DEBUG: Exception in fetchNextBatch() for ${currentRegion.regionName}: ${ex.message}", ex)
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
            // Clean coordinate description by removing parentheses and taking first meaningful part
            val locationName = context.coordinate.description.split("(").first().trim()
            val locationQuery = when {
                context.query in listOf("레스토랑", "음식점", "한식당", "중식당", "일식당", "양식당", "치킨집") -> 
                    "${locationName.split(" ").first()} 맛집"
                else -> "${context.query} ${locationName}"
            }
            
            logger.warn("*** CALLING NAVER API *** query='$locationQuery', display=100, page=${context.page}")
            
            externalApiWebClient.get()
                .uri { builder ->
                    builder
                        .scheme("https")
                        .host("openapi.naver.com")
                        .path("/v1/search/local.json")
                        .queryParam("query", locationQuery)
                        .queryParam("display", 100)
                        .queryParam("start", (context.page - 1) * 100 + 1)
                        .queryParam("sort", "comment")
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
                .doOnSuccess { response ->
                    logger.info("DEBUG: Naver API success for ${context.region.regionName} - query='$locationQuery': got ${response.items.size} items")
                    meterRegistry.counter("regional_naver_api_success", "region", context.region.regionName).increment()
                }
                .doOnError { error ->
                    logger.error("DEBUG: Naver API error for ${context.region.regionName} - query='$locationQuery': ${error.javaClass.simpleName} - ${error.message}", error)
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