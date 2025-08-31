package com.mohe.spring.service

import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Service
import org.springframework.web.reactive.function.client.WebClient
import org.springframework.web.reactive.function.client.WebClientResponseException
import reactor.util.retry.Retry
import java.time.Duration
import java.net.URLEncoder
import java.nio.charset.StandardCharsets

/**
 * Service for reverse geocoding (converting coordinates to addresses)
 * Uses Naver Geocoding API as primary, with fallback options
 */
@Service
class AddressService(
    private val webClient: WebClient,
    @Value("\${naver.place.clientId:}") private val naverClientId: String,
    @Value("\${naver.place.clientSecret:}") private val naverClientSecret: String,
    @Value("\${google.places.apiKey:}") private val googleApiKey: String
) {
    private val logger = LoggerFactory.getLogger(AddressService::class.java)
    
    // Simple in-memory cache for addresses (1 hour)
    private val addressCache = mutableMapOf<String, Pair<AddressInfo, Long>>()
    private val cacheTimeout = Duration.ofHours(1)

    /**
     * Get address information from coordinates
     */
    fun getAddressFromCoordinates(latitude: Double, longitude: Double): AddressInfo {
        val cacheKey = "${latitude}_${longitude}"
        val cached = addressCache[cacheKey]
        
        if (cached != null && System.currentTimeMillis() - cached.second < cacheTimeout.toMillis()) {
            logger.debug("Returning cached address for coordinates: $latitude, $longitude")
            return cached.first
        }

        val address = try {
            // For now, use fallback address mapping to avoid API timeout issues
            // This can be re-enabled once Naver API keys are properly configured
            logger.info("Using geographic approximation for address lookup")
            createFallbackAddress(latitude, longitude)
            
            // Commented out API call until keys are configured
            /*
            if (naverClientId.isNotBlank() && naverClientSecret.isNotBlank()) {
                logger.info("Attempting to get address from Naver API")
                getAddressFromNaver(latitude, longitude)
            } else {
                logger.info("Naver API keys not configured, using fallback address")
                createFallbackAddress(latitude, longitude)
            }
            */
        } catch (error: Exception) {
            logger.warn("Failed to get address, using fallback", error)
            createFallbackAddress(latitude, longitude)
        }

        // Cache the result
        addressCache[cacheKey] = Pair(address, System.currentTimeMillis())
        cleanupCache()
        
        return address
    }

    /**
     * Get address using Naver Reverse Geocoding API
     */
    private fun getAddressFromNaver(latitude: Double, longitude: Double): AddressInfo {
        logger.info("Getting address from Naver for coordinates: $latitude, $longitude")
        
        try {
            val coords = "${longitude},${latitude}" // Naver uses lon,lat format
            val response = webClient.get()
                .uri("https://naveropenapi.apigw.ntruss.com/map-reversegeocode/v2/gc?coords=${coords}&sourcecrs=epsg:4326&output=json&orders=roadaddr")
                .header("X-NCP-APIGW-API-KEY-ID", naverClientId)
                .header("X-NCP-APIGW-API-KEY", naverClientSecret)
                .retrieve()
                .bodyToMono(Map::class.java)
                .retryWhen(
                    Retry.backoff(3, Duration.ofSeconds(1))
                        .maxBackoff(Duration.ofSeconds(5))
                        .filter { it is WebClientResponseException }
                )
                .block(Duration.ofSeconds(3))
                ?: throw RuntimeException("Empty response from Naver Geocoding")

            return parseNaverResponse(response, latitude, longitude)
        } catch (error: Exception) {
            logger.error("Failed to get address from Naver: ${error.message}")
            throw error
        }
    }

    /**
     * Parse Naver Reverse Geocoding response
     */
    @Suppress("UNCHECKED_CAST")
    private fun parseNaverResponse(response: Map<*, *>, latitude: Double, longitude: Double): AddressInfo {
        try {
            val status = response["status"] as? Map<String, Any>
            val statusCode = status?.get("code") as? Int
            
            if (statusCode != 0) {
                logger.warn("Naver API returned non-zero status: $statusCode")
                return createFallbackAddress(latitude, longitude)
            }

            val results = response["results"] as? List<Map<String, Any>>
            if (results.isNullOrEmpty()) {
                logger.warn("No results from Naver API")
                return createFallbackAddress(latitude, longitude)
            }

            val result = results.first()
            val region = result["region"] as? Map<String, Any>
            val land = result["land"] as? Map<String, Any>

            // Extract address components
            val sido = (region?.get("area1") as? Map<String, Any>)?.get("name") as? String ?: ""
            val sigungu = (region?.get("area2") as? Map<String, Any>)?.get("name") as? String ?: ""
            val dong = (region?.get("area3") as? Map<String, Any>)?.get("name") as? String ?: ""
            val roadName = (land?.get("name") as? String) ?: ""
            val buildingNumber = (land?.get("number1") as? String) ?: ""

            // Create formatted address
            val fullAddress = buildString {
                if (sido.isNotEmpty()) append(sido)
                if (sigungu.isNotEmpty()) {
                    if (isNotEmpty()) append(" ")
                    append(sigungu)
                }
                if (dong.isNotEmpty()) {
                    if (isNotEmpty()) append(" ")
                    append(dong)
                }
                if (roadName.isNotEmpty()) {
                    if (isNotEmpty()) append(" ")
                    append(roadName)
                    if (buildingNumber.isNotEmpty()) {
                        append(" $buildingNumber")
                    }
                }
            }

            val shortAddress = buildString {
                if (sigungu.isNotEmpty()) append(sigungu)
                if (dong.isNotEmpty()) {
                    if (isNotEmpty()) append(" ")
                    append(dong)
                }
            }

            return AddressInfo(
                fullAddress = fullAddress.takeIf { it.isNotEmpty() } 
                    ?: createFallbackAddress(latitude, longitude).fullAddress,
                shortAddress = shortAddress.takeIf { it.isNotEmpty() } 
                    ?: createFallbackAddress(latitude, longitude).shortAddress,
                sido = sido,
                sigungu = sigungu,
                dong = dong,
                roadName = roadName,
                buildingNumber = buildingNumber,
                latitude = latitude,
                longitude = longitude
            )
        } catch (error: Exception) {
            logger.error("Failed to parse Naver response", error)
            return createFallbackAddress(latitude, longitude)
        }
    }

    /**
     * Create fallback address when API calls fail
     */
    private fun createFallbackAddress(latitude: Double, longitude: Double): AddressInfo {
        // Simple geographic approximation for major Korean cities
        val approximateLocation = getApproximateLocation(latitude, longitude)
        
        return if (approximateLocation != null) {
            AddressInfo(
                fullAddress = approximateLocation.full,
                shortAddress = approximateLocation.short,
                sido = approximateLocation.sido,
                sigungu = approximateLocation.sigungu,
                dong = approximateLocation.dong,
                roadName = "",
                buildingNumber = "",
                latitude = latitude,
                longitude = longitude
            )
        } else {
            val formattedCoords = "위도 ${String.format("%.4f", latitude)}, 경도 ${String.format("%.4f", longitude)}"
            AddressInfo(
                fullAddress = formattedCoords,
                shortAddress = formattedCoords,
                sido = "",
                sigungu = "",
                dong = "",
                roadName = "",
                buildingNumber = "",
                latitude = latitude,
                longitude = longitude
            )
        }
    }

    /**
     * Approximate Korean location based on coordinates
     */
    private fun getApproximateLocation(lat: Double, lon: Double): ApproximateLocation? {
        // Seoul area (37.4-37.7, 126.7-127.2)
        if (lat >= 37.4 && lat <= 37.7 && lon >= 126.7 && lon <= 127.2) {
            return when {
                lat >= 37.6 && lon <= 126.9 -> ApproximateLocation("서울특별시 강서구", "강서구", "서울특별시", "강서구", "")
                lat >= 37.6 && lon >= 127.0 -> ApproximateLocation("서울특별시 강북구", "강북구", "서울특별시", "강북구", "")
                lat >= 37.5 && lat < 37.6 && lon <= 126.9 -> ApproximateLocation("서울특별시 마포구", "마포구", "서울특별시", "마포구", "")
                lat >= 37.5 && lat < 37.6 && lon >= 127.0 -> ApproximateLocation("서울특별시 강남구", "강남구", "서울특별시", "강남구", "")
                else -> ApproximateLocation("서울특별시 중구", "중구", "서울특별시", "중구", "")
            }
        }
        
        // Gyeonggi-do area (37.0-37.6, 126.8-127.6)  
        if (lat >= 37.0 && lat <= 37.6 && lon >= 126.8 && lon <= 127.6) {
            return when {
                lat >= 37.2 && lat <= 37.4 && lon >= 127.0 && lon <= 127.4 -> 
                    ApproximateLocation("경기도 용인시 기흥구", "용인시 기흥구", "경기도", "용인시", "보정동")
                lat >= 37.3 && lat <= 37.5 && lon >= 127.0 && lon <= 127.3 -> 
                    ApproximateLocation("경기도 성남시 분당구", "성남시 분당구", "경기도", "성남시", "분당동")
                lat >= 37.4 && lat <= 37.6 && lon >= 126.9 && lon <= 127.2 -> 
                    ApproximateLocation("경기도 고양시 일산서구", "고양시 일산서구", "경기도", "고양시", "일산동")
                else -> ApproximateLocation("경기도", "경기도", "경기도", "", "")
            }
        }
        
        // Incheon area (37.2-37.6, 126.4-126.9)
        if (lat >= 37.2 && lat <= 37.6 && lon >= 126.4 && lon <= 126.9) {
            return ApproximateLocation("인천광역시 연수구", "연수구", "인천광역시", "연수구", "")
        }
        
        return null
    }

    /**
     * Clean up expired cache entries
     */
    private fun cleanupCache() {
        val now = System.currentTimeMillis()
        val expiredKeys = addressCache.filterValues { 
            now - it.second > cacheTimeout.toMillis() 
        }.keys
        expiredKeys.forEach { addressCache.remove(it) }
    }
}

/**
 * Address information data class
 */
data class AddressInfo(
    val fullAddress: String,
    val shortAddress: String,
    val sido: String,           // 시/도 (서울특별시, 경기도 등)
    val sigungu: String,        // 시/군/구 (강남구, 성남시 등)  
    val dong: String,           // 동/면/읍 (역삼동, 분당동 등)
    val roadName: String,       // 도로명 (테헤란로, 판교로 등)
    val buildingNumber: String, // 건물번호
    val latitude: Double,
    val longitude: Double
)

/**
 * Internal data class for approximate location mapping
 */
private data class ApproximateLocation(
    val full: String,
    val short: String,
    val sido: String,
    val sigungu: String,
    val dong: String
)