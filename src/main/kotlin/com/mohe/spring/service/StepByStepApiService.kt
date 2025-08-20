package com.mohe.spring.service

import com.example.ingestion.dto.GooglePlaceDetail
import com.example.ingestion.dto.NaverPlaceItem
import com.fasterxml.jackson.databind.ObjectMapper
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.http.HttpStatus
import org.springframework.stereotype.Service
import org.springframework.web.reactive.function.client.WebClient
import reactor.core.publisher.Mono
import java.time.Duration
import java.util.concurrent.CompletableFuture
import java.util.concurrent.Executor
import java.util.concurrent.Executors
import kotlin.math.min

/**
 * 단계별 API 호출 관리 서비스
 * 
 * 기존의 배치 방식과 달리 다음과 같은 기능을 제공합니다:
 * - 개별 장소에 대한 단계별 API 호출
 * - 실패한 API 호출에 대한 개별 재시도
 * - API 호출 진행 상황 추적
 * - 미래 확장을 위한 모듈화된 구조
 */
@Service
class StepByStepApiService(
    private val webClient: WebClient,
    private val objectMapper: ObjectMapper,
    @Value("\${app.external.naver.base-url}") private val naverBaseUrl: String,
    @Value("\${app.external.naver.client-id}") private val naverClientId: String,
    @Value("\${app.external.naver.client-secret}") private val naverClientSecret: String,
    @Value("\${app.external.google.base-url}") private val googleBaseUrl: String,
    @Value("\${app.external.google.api-key}") private val googleApiKey: String,
    @Value("\${app.api.retry.max-attempts:3}") private val maxRetryAttempts: Int,
    @Value("\${app.api.retry.delay-ms:1000}") private val retryDelayMs: Long,
    @Value("\${app.api.timeout.naver:10}") private val naverTimeoutSeconds: Int,
    @Value("\${app.api.timeout.google:15}") private val googleTimeoutSeconds: Int
) {

    private val logger = LoggerFactory.getLogger(StepByStepApiService::class.java)
    private val apiExecutor: Executor = Executors.newFixedThreadPool(5)

    data class ApiCallStep(
        val stepName: String,
        val status: StepStatus,
        val result: Any? = null,
        val error: String? = null,
        val attemptCount: Int = 0,
        val maxAttempts: Int = 3,
        val processingTimeMs: Long = 0
    )

    enum class StepStatus {
        PENDING, IN_PROGRESS, SUCCESS, FAILED, RETRY_NEEDED
    }

    data class PlaceProcessingProgress(
        val placeId: String,
        val placeName: String,
        val steps: MutableMap<String, ApiCallStep> = mutableMapOf(),
        val overallStatus: StepStatus = StepStatus.PENDING,
        val startTime: Long = System.currentTimeMillis(),
        val completionTime: Long? = null,
        val totalSteps: Int = 0,
        val completedSteps: Int = 0
    ) {
        fun getProgressPercentage(): Int = if (totalSteps == 0) 0 else (completedSteps * 100) / totalSteps
        
        fun getTotalProcessingTime(): Long = completionTime?.let { it - startTime } ?: (System.currentTimeMillis() - startTime)
    }

    /**
     * 개별 장소에 대해 순차적으로 API 호출을 실행합니다.
     */
    suspend fun processPlaceStepByStep(
        placeName: String, 
        address: String? = null,
        latitude: Double? = null,
        longitude: Double? = null
    ): PlaceProcessingProgress {
        
        val placeId = generatePlaceId(placeName, address)
        val progress = PlaceProcessingProgress(
            placeId = placeId,
            placeName = placeName,
            totalSteps = 3 // Naver search, Google search, Google details
        )
        
        logger.info("Starting step-by-step processing for place: $placeName")
        
        try {
            // Step 1: Naver Place Search
            executeStep(progress, "NAVER_SEARCH") {
                fetchFromNaver(placeName, address)
            }
            
            // Step 2: Google Places Search (if Naver was successful)
            val naverResult = progress.steps["NAVER_SEARCH"]?.result as? NaverPlaceItem
            if (naverResult != null) {
                executeStep(progress, "GOOGLE_SEARCH") {
                    searchGooglePlace(naverResult)
                }
                
                // Step 3: Google Place Details (if search was successful)
                val googleSearchResult = progress.steps["GOOGLE_SEARCH"]?.result as? String
                if (googleSearchResult != null) {
                    executeStep(progress, "GOOGLE_DETAILS") {
                        fetchGooglePlaceDetails(googleSearchResult)
                    }
                }
            }
            
            progress.overallStatus = determineOverallStatus(progress)
            progress.completionTime = System.currentTimeMillis()
            
        } catch (e: Exception) {
            logger.error("Fatal error processing place $placeName: ${e.message}", e)
            progress.overallStatus = StepStatus.FAILED
            progress.completionTime = System.currentTimeMillis()
        }
        
        logger.info("Completed processing place: $placeName, Status: ${progress.overallStatus}, " +
                   "Progress: ${progress.getProgressPercentage()}%, " +
                   "Time: ${progress.getTotalProcessingTime()}ms")
        
        return progress
    }

    /**
     * 실패한 단계들에 대해서만 재처리를 실행합니다.
     */
    suspend fun retryFailedSteps(progress: PlaceProcessingProgress): PlaceProcessingProgress {
        logger.info("Retrying failed steps for place: ${progress.placeName}")
        
        val failedSteps = progress.steps.filter { 
            it.value.status == StepStatus.FAILED || it.value.status == StepStatus.RETRY_NEEDED 
        }
        
        for ((stepName, step) in failedSteps) {
            if (step.attemptCount < step.maxAttempts) {
                logger.info("Retrying step $stepName for ${progress.placeName} (attempt ${step.attemptCount + 1})")
                
                when (stepName) {
                    "NAVER_SEARCH" -> executeStep(progress, stepName) { 
                        fetchFromNaver(progress.placeName, null) 
                    }
                    "GOOGLE_SEARCH" -> {
                        val naverResult = progress.steps["NAVER_SEARCH"]?.result as? NaverPlaceItem
                        if (naverResult != null) {
                            executeStep(progress, stepName) { 
                                searchGooglePlace(naverResult) 
                            }
                        }
                    }
                    "GOOGLE_DETAILS" -> {
                        val placeId = progress.steps["GOOGLE_SEARCH"]?.result as? String
                        if (placeId != null) {
                            executeStep(progress, stepName) { 
                                fetchGooglePlaceDetails(placeId) 
                            }
                        }
                    }
                }
                
                // Rate limiting between retries
                Thread.sleep(retryDelayMs)
            }
        }
        
        progress.overallStatus = determineOverallStatus(progress)
        return progress
    }

    /**
     * 여러 장소를 병렬로 처리합니다 (각 장소는 내부적으로 순차 처리)
     */
    fun processMultiplePlaces(places: List<Pair<String, String?>>): List<CompletableFuture<PlaceProcessingProgress>> {
        logger.info("Starting parallel processing for ${places.size} places")
        
        return places.map { (placeName, address) ->
            CompletableFuture.supplyAsync({
                runBlocking {
                    processPlaceStepByStep(placeName, address)
                }
            }, apiExecutor)
        }
    }

    /**
     * 단일 스텝 실행 및 결과 추적
     */
    private suspend fun executeStep(
        progress: PlaceProcessingProgress, 
        stepName: String, 
        apiCall: suspend () -> Any?
    ) {
        val startTime = System.currentTimeMillis()
        val existingStep = progress.steps[stepName]
        val attemptCount = (existingStep?.attemptCount ?: 0) + 1
        
        progress.steps[stepName] = ApiCallStep(
            stepName = stepName,
            status = StepStatus.IN_PROGRESS,
            attemptCount = attemptCount,
            maxAttempts = maxRetryAttempts
        )
        
        try {
            val result = apiCall()
            val processingTime = System.currentTimeMillis() - startTime
            
            progress.steps[stepName] = ApiCallStep(
                stepName = stepName,
                status = StepStatus.SUCCESS,
                result = result,
                attemptCount = attemptCount,
                maxAttempts = maxRetryAttempts,
                processingTimeMs = processingTime
            )
            
            progress.completedSteps = progress.steps.values.count { it.status == StepStatus.SUCCESS }
            logger.debug("Step $stepName completed successfully in ${processingTime}ms")
            
        } catch (e: Exception) {
            val processingTime = System.currentTimeMillis() - startTime
            val status = if (attemptCount < maxRetryAttempts) StepStatus.RETRY_NEEDED else StepStatus.FAILED
            
            progress.steps[stepName] = ApiCallStep(
                stepName = stepName,
                status = status,
                error = e.message,
                attemptCount = attemptCount,
                maxAttempts = maxRetryAttempts,
                processingTimeMs = processingTime
            )
            
            logger.warn("Step $stepName failed (attempt $attemptCount): ${e.message}")
        }
    }

    /**
     * 네이버 API 호출
     */
    private suspend fun fetchFromNaver(placeName: String, address: String?): NaverPlaceItem {
        val searchQuery = if (address != null) "$placeName $address" else placeName
        
        return webClient.get()
            .uri { builder ->
                builder
                    .path(naverBaseUrl)
                    .queryParam("query", searchQuery)
                    .queryParam("display", 1)
                    .build()
            }
            .header("X-Naver-Client-Id", naverClientId)
            .header("X-Naver-Client-Secret", naverClientSecret)
            .retrieve()
            .bodyToMono(com.example.ingestion.dto.NaverLocalSearchResponse::class.java)
            .timeout(Duration.ofSeconds(naverTimeoutSeconds.toLong()))
            .block()
            ?.items?.firstOrNull()
            ?: throw RuntimeException("No results found from Naver API for: $placeName")
    }

    /**
     * 구글 장소 검색 API 호출
     */
    private suspend fun searchGooglePlace(naverPlace: NaverPlaceItem): String {
        val response = webClient.get()
            .uri { builder ->
                builder
                    .path("$googleBaseUrl/nearbysearch/json")
                    .queryParam("location", "${naverPlace.latitude},${naverPlace.longitude}")
                    .queryParam("radius", 100)
                    .queryParam("keyword", naverPlace.cleanTitle)
                    .queryParam("key", googleApiKey)
                    .queryParam("language", "ko")
                    .build()
            }
            .retrieve()
            .bodyToMono(com.example.ingestion.dto.GoogleNearbySearchResponse::class.java)
            .timeout(Duration.ofSeconds(googleTimeoutSeconds.toLong()))
            .block()
            
        return response?.results?.firstOrNull()?.placeId
            ?: throw RuntimeException("No Google place found for: ${naverPlace.cleanTitle}")
    }

    /**
     * 구글 장소 상세 정보 API 호출
     */
    private suspend fun fetchGooglePlaceDetails(placeId: String): GooglePlaceDetail {
        return webClient.get()
            .uri { builder ->
                builder
                    .path("$googleBaseUrl/details/json")
                    .queryParam("place_id", placeId)
                    .queryParam("fields", "place_id,name,formatted_address,formatted_phone_number,website,opening_hours,rating,user_ratings_total,price_level,types,photos,reviews")
                    .queryParam("key", googleApiKey)
                    .queryParam("language", "ko")
                    .build()
            }
            .retrieve()
            .bodyToMono(com.example.ingestion.dto.GooglePlaceDetailsResponse::class.java)
            .timeout(Duration.ofSeconds(googleTimeoutSeconds.toLong()))
            .block()
            ?.result
            ?: throw RuntimeException("Failed to get Google place details for: $placeId")
    }

    /**
     * 전체 진행 상태 결정
     */
    private fun determineOverallStatus(progress: PlaceProcessingProgress): StepStatus {
        val steps = progress.steps.values
        return when {
            steps.any { it.status == StepStatus.IN_PROGRESS } -> StepStatus.IN_PROGRESS
            steps.all { it.status == StepStatus.SUCCESS } -> StepStatus.SUCCESS
            steps.any { it.status == StepStatus.RETRY_NEEDED } -> StepStatus.RETRY_NEEDED
            else -> StepStatus.FAILED
        }
    }

    private fun generatePlaceId(placeName: String, address: String?): String {
        val combined = if (address != null) "${placeName}_$address" else placeName
        return "step_${combined.hashCode().toString().replace("-", "n")}"
    }

    // Extension function for coroutines
    private suspend fun <T> runBlocking(block: suspend () -> T): T {
        return kotlinx.coroutines.runBlocking { block() }
    }
}