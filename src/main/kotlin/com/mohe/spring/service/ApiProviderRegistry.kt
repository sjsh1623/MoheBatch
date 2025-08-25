package com.mohe.spring.service

// Removed external batch DTO imports - using generic Any type instead
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import java.util.concurrent.ConcurrentHashMap

/**
 * API 제공자 등록소 - 미래 확장을 위한 모듈화된 구조
 * 
 * 이 서비스는 다양한 API 제공자들을 동적으로 등록하고 관리합니다.
 * 새로운 API (예: Kakao Map, Foursquare 등)를 추가할 때 기존 코드 수정 없이
 * 플러그인 방식으로 확장할 수 있습니다.
 */
@Service
class ApiProviderRegistry {

    private val logger = LoggerFactory.getLogger(ApiProviderRegistry::class.java)
    private val providers = ConcurrentHashMap<String, ApiProvider>()
    private val processingStrategies = ConcurrentHashMap<String, PlaceProcessingStrategy>()

    init {
        // 기본 제공자들 등록
        registerProvider("naver", NaverApiProvider())
        registerProvider("google", GoogleApiProvider())
        
        // 기본 처리 전략들 등록
        registerProcessingStrategy("food_focus", FoodFocusedStrategy())
        registerProcessingStrategy("atmosphere_focus", AtmosphereBasedStrategy())
        registerProcessingStrategy("balanced", BalancedStrategy())
    }

    /**
     * 새로운 API 제공자 등록
     */
    fun registerProvider(providerName: String, provider: ApiProvider) {
        providers[providerName] = provider
        logger.info("Registered API provider: $providerName (${provider.javaClass.simpleName})")
    }

    /**
     * 새로운 처리 전략 등록
     */
    fun registerProcessingStrategy(strategyName: String, strategy: PlaceProcessingStrategy) {
        processingStrategies[strategyName] = strategy
        logger.info("Registered processing strategy: $strategyName (${strategy.javaClass.simpleName})")
    }

    /**
     * 등록된 제공자 목록 조회
     */
    fun getAvailableProviders(): Set<String> = providers.keys.toSet()

    /**
     * 등록된 처리 전략 목록 조회
     */
    fun getAvailableStrategies(): Set<String> = processingStrategies.keys.toSet()

    /**
     * 특정 제공자 조회
     */
    fun getProvider(providerName: String): ApiProvider? = providers[providerName]

    /**
     * 특정 처리 전략 조회
     */
    fun getProcessingStrategy(strategyName: String): PlaceProcessingStrategy? = processingStrategies[strategyName]

    /**
     * 모든 등록된 제공자를 사용해 장소 정보 수집
     */
    suspend fun fetchFromAllProviders(
        placeName: String, 
        address: String? = null,
        providerFilter: Set<String>? = null
    ): Map<String, ApiResult> {
        
        val targetProviders = providerFilter?.let { filter ->
            providers.filterKeys { it in filter }
        } ?: providers

        val results = mutableMapOf<String, ApiResult>()

        for ((providerName, provider) in targetProviders) {
            try {
                logger.debug("Fetching data from provider: $providerName for place: $placeName")
                val result = provider.fetchPlaceData(placeName, address)
                results[providerName] = ApiResult.success(result)
                logger.debug("Successfully fetched data from $providerName")
            } catch (e: Exception) {
                logger.warn("Failed to fetch data from $providerName: ${e.message}")
                results[providerName] = ApiResult.error(e.message ?: "Unknown error")
            }
        }

        return results
    }

    /**
     * 여러 제공자의 데이터를 지정된 전략으로 병합
     */
    fun mergeProviderData(
        providerResults: Map<String, ApiResult>,
        strategyName: String = "balanced"
    ): PlaceMergeResult {
        
        val strategy = getProcessingStrategy(strategyName) 
            ?: throw IllegalArgumentException("Unknown processing strategy: $strategyName")
        
        val successfulResults = providerResults.filterValues { it.isSuccess() }
        
        return if (successfulResults.isNotEmpty()) {
            try {
                strategy.mergeResults(successfulResults.mapValues { it.value.data!! })
            } catch (e: Exception) {
                logger.error("Failed to merge provider data with strategy $strategyName: ${e.message}", e)
                PlaceMergeResult.error("Merge failed: ${e.message}")
            }
        } else {
            PlaceMergeResult.error("No successful provider results to merge")
        }
    }
}

/**
 * API 제공자 인터페이스
 */
interface ApiProvider {
    /**
     * 제공자 이름
     */
    fun getProviderName(): String

    /**
     * 지원하는 데이터 타입들 (description, photos, reviews, hours 등)
     */
    fun getSupportedDataTypes(): Set<String>

    /**
     * 장소 데이터 가져오기
     */
    suspend fun fetchPlaceData(placeName: String, address: String? = null): Any

    /**
     * 제공자별 설정 검증
     */
    fun validateConfiguration(): Boolean
}

/**
 * 장소 데이터 처리 전략 인터페이스
 */
interface PlaceProcessingStrategy {
    /**
     * 전략 이름
     */
    fun getStrategyName(): String

    /**
     * 여러 제공자의 결과를 병합
     */
    fun mergeResults(providerData: Map<String, Any>): PlaceMergeResult

    /**
     * 전략에 적합한 장소 타입 판단
     */
    fun isApplicableFor(placeCategory: String, providerData: Map<String, Any>): Boolean
}

/**
 * API 호출 결과 래퍼
 */
data class ApiResult(
    val success: Boolean,
    val data: Any? = null,
    val error: String? = null,
    val responseTime: Long = 0,
    val providerName: String = ""
) {
    fun isSuccess(): Boolean = success
    fun isError(): Boolean = !success

    companion object {
        fun success(data: Any, responseTime: Long = 0, providerName: String = ""): ApiResult {
            return ApiResult(success = true, data = data, responseTime = responseTime, providerName = providerName)
        }

        fun error(errorMessage: String, responseTime: Long = 0, providerName: String = ""): ApiResult {
            return ApiResult(success = false, error = errorMessage, responseTime = responseTime, providerName = providerName)
        }
    }
}

/**
 * 병합 결과 데이터
 */
data class PlaceMergeResult(
    val success: Boolean,
    val mergedData: MergedPlaceData? = null,
    val error: String? = null,
    val sourceProviders: Set<String> = emptySet(),
    val processingStrategy: String = ""
) {
    companion object {
        fun success(
            mergedData: MergedPlaceData, 
            sourceProviders: Set<String>, 
            strategy: String
        ): PlaceMergeResult {
            return PlaceMergeResult(
                success = true, 
                mergedData = mergedData, 
                sourceProviders = sourceProviders,
                processingStrategy = strategy
            )
        }

        fun error(errorMessage: String): PlaceMergeResult {
            return PlaceMergeResult(success = false, error = errorMessage)
        }
    }
}

/**
 * 병합된 장소 데이터 표현
 */
data class MergedPlaceData(
    val name: String,
    val description: String,
    val category: String,
    val address: String? = null,
    val phone: String? = null,
    val website: String? = null,
    val rating: Double? = null,
    val photos: List<String> = emptyList(),
    val openingHours: String? = null,
    val priceLevel: Int? = null,
    val amenities: List<String> = emptyList(),
    val sourceMetadata: Map<String, Any> = emptyMap()
)

// 기본 구현체들
class NaverApiProvider : ApiProvider {
    override fun getProviderName(): String = "naver"
    
    override fun getSupportedDataTypes(): Set<String> = setOf("description", "category", "address", "phone")
    
    override suspend fun fetchPlaceData(placeName: String, address: String?): Any {
        // 실제 구현은 기존 Naver API 호출 로직 사용
        throw NotImplementedError("Naver API integration to be implemented")
    }
    
    override fun validateConfiguration(): Boolean = true // API key validation logic
}

class GoogleApiProvider : ApiProvider {
    override fun getProviderName(): String = "google"
    
    override fun getSupportedDataTypes(): Set<String> = setOf("description", "photos", "reviews", "hours", "rating")
    
    override suspend fun fetchPlaceData(placeName: String, address: String?): Any {
        // 실제 구현은 기존 Google API 호출 로직 사용
        throw NotImplementedError("Google API integration to be implemented")
    }
    
    override fun validateConfiguration(): Boolean = true // API key validation logic
}

// 처리 전략 구현체들
class FoodFocusedStrategy : PlaceProcessingStrategy {
    override fun getStrategyName(): String = "food_focused"
    
    override fun mergeResults(providerData: Map<String, Any>): PlaceMergeResult {
        // 음식점에 특화된 데이터 병합 로직
        throw NotImplementedError("Food-focused merge strategy to be implemented")
    }
    
    override fun isApplicableFor(placeCategory: String, providerData: Map<String, Any>): Boolean {
        return placeCategory.contains("음식") || placeCategory.contains("카페") || placeCategory.contains("레스토랑")
    }
}

class AtmosphereBasedStrategy : PlaceProcessingStrategy {
    override fun getStrategyName(): String = "atmosphere_based"
    
    override fun mergeResults(providerData: Map<String, Any>): PlaceMergeResult {
        // 분위기 중심의 데이터 병합 로직
        throw NotImplementedError("Atmosphere-based merge strategy to be implemented")
    }
    
    override fun isApplicableFor(placeCategory: String, providerData: Map<String, Any>): Boolean {
        return placeCategory.contains("카페") || placeCategory.contains("문화") || placeCategory.contains("박물관")
    }
}

class BalancedStrategy : PlaceProcessingStrategy {
    override fun getStrategyName(): String = "balanced"
    
    override fun mergeResults(providerData: Map<String, Any>): PlaceMergeResult {
        // 균형잡힌 범용 데이터 병합 로직
        throw NotImplementedError("Balanced merge strategy to be implemented")
    }
    
    override fun isApplicableFor(placeCategory: String, providerData: Map<String, Any>): Boolean {
        return true // 모든 카테고리에 적용 가능
    }
}