package com.mohe.spring.controller

import com.mohe.spring.dto.ApiResponse
import com.mohe.spring.service.EnhancedRecommendationService
import com.mohe.spring.service.EnhancedRecommendationsResponse
import com.mohe.spring.service.ContextualRecommendationService
import com.mohe.spring.service.ContextualRecommendationRequest
import com.mohe.spring.service.ContextualRecommendationResponse
import com.mohe.spring.service.WeatherService
import com.mohe.spring.security.UserPrincipal
import com.mohe.spring.repository.UserRepository
import com.mohe.spring.repository.PlaceRepository
import com.mohe.spring.repository.BookmarkRepository
import java.time.LocalTime
import org.springframework.data.domain.PageRequest
import io.swagger.v3.oas.annotations.Operation
import io.swagger.v3.oas.annotations.Parameter
import io.swagger.v3.oas.annotations.responses.ApiResponse as SwaggerApiResponse
import io.swagger.v3.oas.annotations.responses.ApiResponses
import io.swagger.v3.oas.annotations.tags.Tag
import org.slf4j.LoggerFactory
import org.springframework.http.ResponseEntity
import org.springframework.security.core.annotation.AuthenticationPrincipal
import org.springframework.web.bind.annotation.*

/**
 * Enhanced recommendations controller using MBTI-weighted similarity calculations
 */
@RestController
@RequestMapping("/api/recommendations")
@Tag(name = "Enhanced Recommendations", description = "MBTI-weighted similarity-based place recommendations")
class RecommendationController(
    private val enhancedRecommendationService: EnhancedRecommendationService,
    private val contextualRecommendationService: ContextualRecommendationService,
    private val weatherService: WeatherService,
    private val userRepository: UserRepository,
    private val placeRepository: PlaceRepository,
    private val bookmarkRepository: BookmarkRepository
) {

    private val logger = LoggerFactory.getLogger(RecommendationController::class.java)

    @Operation(
        summary = "Get enhanced recommendations",
        description = "Get personalized place recommendations using MBTI-weighted similarity calculations based on user's bookmark history"
    )
    @ApiResponses(
        value = [
            SwaggerApiResponse(responseCode = "200", description = "Enhanced recommendations retrieved successfully"),
            SwaggerApiResponse(responseCode = "400", description = "Invalid parameters"),
            SwaggerApiResponse(responseCode = "401", description = "User not authenticated"),
            SwaggerApiResponse(responseCode = "500", description = "Failed to generate recommendations")
        ]
    )
    @GetMapping("/enhanced")
    fun getEnhancedRecommendations(
        @AuthenticationPrincipal userPrincipal: UserPrincipal,
        @Parameter(description = "Maximum number of recommendations to return (default: 15, max: 50)")
        @RequestParam(defaultValue = "15") limit: Int,
        @Parameter(description = "Whether to exclude already bookmarked places (default: true)")
        @RequestParam(defaultValue = "true") excludeBookmarked: Boolean
    ): ResponseEntity<ApiResponse<EnhancedRecommendationsResponse>> {
        return try {
            if (limit < 1 || limit > 50) {
                return ResponseEntity.badRequest().body(
                    ApiResponse.error("VALIDATION_ERROR", "Limit must be between 1 and 50")
                )
            }

            val user = userRepository.findById(userPrincipal.id)
                .orElseThrow { RuntimeException("사용자를 찾을 수 없습니다") }

            logger.info("Generating enhanced recommendations for user ${user.id} with limit $limit")

            val recommendations = enhancedRecommendationService.getEnhancedRecommendations(
                user = user,
                limit = limit,
                excludeBookmarked = excludeBookmarked
            )

            ResponseEntity.ok(ApiResponse.success(recommendations))

        } catch (ex: Exception) {
            logger.error("Failed to get enhanced recommendations for user ${userPrincipal.id}", ex)
            ResponseEntity.status(500).body(
                ApiResponse.error("INTERNAL_SERVER_ERROR", "Failed to generate recommendations: ${ex.message}")
            )
        }
    }

    @Operation(
        summary = "Get MBTI-specific recommendations",
        description = "Get recommendations specifically tailored to a particular MBTI type (useful for exploring different personality preferences)"
    )
    @ApiResponses(
        value = [
            SwaggerApiResponse(responseCode = "200", description = "MBTI-specific recommendations retrieved successfully"),
            SwaggerApiResponse(responseCode = "400", description = "Invalid MBTI type or parameters"),
            SwaggerApiResponse(responseCode = "401", description = "User not authenticated"),
            SwaggerApiResponse(responseCode = "500", description = "Failed to generate recommendations")
        ]
    )
    @GetMapping("/mbti/{mbtiType}")
    fun getMbtiSpecificRecommendations(
        @AuthenticationPrincipal userPrincipal: UserPrincipal,
        @Parameter(description = "MBTI type (e.g., INTJ, ENFP)", required = true)
        @PathVariable mbtiType: String,
        @Parameter(description = "Maximum number of recommendations to return (default: 15, max: 50)")
        @RequestParam(defaultValue = "15") limit: Int
    ): ResponseEntity<ApiResponse<EnhancedRecommendationsResponse>> {
        return try {
            if (!isValidMbtiType(mbtiType)) {
                return ResponseEntity.badRequest().body(
                    ApiResponse.error("VALIDATION_ERROR", "Invalid MBTI type: $mbtiType")
                )
            }

            if (limit < 1 || limit > 50) {
                return ResponseEntity.badRequest().body(
                    ApiResponse.error("VALIDATION_ERROR", "Limit must be between 1 and 50")
                )
            }

            val user = userRepository.findById(userPrincipal.id)
                .orElseThrow { RuntimeException("사용자를 찾을 수 없습니다") }

            // Create a temporary user object with the specified MBTI type
            val mbtiUser = user.copy(mbti = mbtiType.uppercase())

            logger.info("Generating MBTI-specific recommendations for user ${user.id} with MBTI $mbtiType")

            val recommendations = enhancedRecommendationService.getEnhancedRecommendations(
                user = mbtiUser,
                limit = limit,
                excludeBookmarked = true
            )

            ResponseEntity.ok(ApiResponse.success(recommendations))

        } catch (ex: Exception) {
            logger.error("Failed to get MBTI-specific recommendations for user ${userPrincipal.id} with MBTI $mbtiType", ex)
            ResponseEntity.status(500).body(
                ApiResponse.error("INTERNAL_SERVER_ERROR", "Failed to generate MBTI-specific recommendations: ${ex.message}")
            )
        }
    }

    @Operation(
        summary = "Get recommendation explanation",
        description = "Get detailed explanation of why specific places were recommended to the user"
    )
    @ApiResponses(
        value = [
            SwaggerApiResponse(responseCode = "200", description = "Recommendation explanation retrieved successfully"),
            SwaggerApiResponse(responseCode = "401", description = "User not authenticated"),
            SwaggerApiResponse(responseCode = "500", description = "Failed to generate explanation")
        ]
    )
    @GetMapping("/explanation")
    fun getRecommendationExplanation(
        @AuthenticationPrincipal userPrincipal: UserPrincipal
    ): ResponseEntity<ApiResponse<Map<String, Any>>> {
        return try {
            val user = userRepository.findById(userPrincipal.id)
                .orElseThrow { RuntimeException("사용자를 찾을 수 없습니다") }

            val explanation = mapOf(
                "userId" to user.id,
                "userMbti" to (user.mbti ?: "unknown"),
                "algorithm" to "mbti_similarity_based",
                "explanation" to mapOf(
                    "step1" to "사용자의 북마크된 장소들을 분석합니다",
                    "step2" to "북마크된 장소와 유사한 다른 장소들을 찾습니다",
                    "step3" to "MBTI 성향에 따라 가중치를 적용합니다",
                    "step4" to "다양성과 인기도 균형을 맞춰 최종 추천 목록을 생성합니다"
                ),
                "factors" to mapOf(
                    "similarity" to "북마크 기반 유사도 (자카드, 코사인 유사도)",
                    "mbti" to "MBTI 성향별 장소 선호도 가중치",
                    "diversity" to "카테고리와 지역 다양성 보장",
                    "freshness" to "최근 데이터에 더 높은 가중치 부여",
                    "popularity" to "인기 편향 완화를 위한 패널티 적용"
                )
            )

            ResponseEntity.ok(ApiResponse.success(explanation))

        } catch (ex: Exception) {
            logger.error("Failed to get recommendation explanation for user ${userPrincipal.id}", ex)
            ResponseEntity.status(500).body(
                ApiResponse.error("INTERNAL_SERVER_ERROR", "Failed to generate explanation: ${ex.message}")
            )
        }
    }

    /**
     * Validate MBTI type format
     */
    @Operation(
        summary = "Get contextual recommendations (dual mode)",
        description = """
        Get place recommendations based on weather, time, and location:
        - For authenticated users: Uses MBTI + bookmarks + weather + time
        - For guest users: Uses popular bookmarked places + weather + time
        """
    )
    @ApiResponses(
        value = [
            SwaggerApiResponse(responseCode = "200", description = "Recommendations retrieved successfully"),
            SwaggerApiResponse(responseCode = "400", description = "Invalid parameters"),
            SwaggerApiResponse(responseCode = "500", description = "Failed to generate recommendations")
        ]
    )
    @GetMapping("/contextual")
    fun getContextualRecommendations(
        @Parameter(description = "User latitude", required = true)
        @RequestParam lat: Double,
        @Parameter(description = "User longitude", required = true)
        @RequestParam lon: Double,
        @Parameter(description = "Search query or keywords", required = false)
        @RequestParam(required = false) query: String?,
        @Parameter(description = "Maximum number of recommendations (default: 10, max: 20)")
        @RequestParam(defaultValue = "10") limit: Int,
        @AuthenticationPrincipal userPrincipal: UserPrincipal?
    ): ResponseEntity<ApiResponse<ContextualRecommendationResponse>> {
        return try {
            // Validate inputs
            if (limit > 20) {
                return ResponseEntity.badRequest().body(
                    ApiResponse.error("INVALID_LIMIT", "Limit cannot exceed 20")
                )
            }

            val timeContext = getCurrentTimeContext()
            logger.info("Processing contextual recommendations for authenticated=${userPrincipal != null}, lat=$lat, lon=$lon")

            val response = if (userPrincipal != null) {
                // Authenticated user: Use MBTI + bookmarks + weather + time
                getPersonalizedContextualRecommendations(userPrincipal, lat, lon, query, timeContext, limit)
            } else {
                // Guest user: Use popular places + weather + time
                getGuestContextualRecommendations(lat, lon, query, timeContext, limit)
            }

            ResponseEntity.ok(ApiResponse.success(response))

        } catch (e: Exception) {
            logger.error("Failed to generate contextual recommendations", e)
            ResponseEntity.status(500).body(
                ApiResponse.error("RECOMMENDATION_ERROR", "Failed to generate recommendations: ${e.message}")
            )
        }
    }

    private fun getPersonalizedContextualRecommendations(
        userPrincipal: UserPrincipal,
        lat: Double,
        lon: Double,
        query: String?,
        timeContext: String,
        limit: Int
    ): ContextualRecommendationResponse {
        logger.info("Getting personalized recommendations for user ${userPrincipal.id}")
        
        // Get user entity for enhanced recommendations
        val user = userRepository.findById(userPrincipal.id).orElse(null)
        if (user == null) {
            logger.warn("User ${userPrincipal.id} not found, using guest recommendations")
            return getGuestContextualRecommendations(lat, lon, query, timeContext, limit)
        }
        
        // Use existing enhanced recommendation system enhanced with weather/time context
        val enhancedResponse = enhancedRecommendationService.getEnhancedRecommendations(
            user, limit * 2, true // Get more results to filter by location
        )
        
        // Filter by location proximity and add contextual information
        val weatherContext = try {
            weatherService.getWeatherContext(lat, lon)
        } catch (e: Exception) {
            logger.warn("Failed to get weather context", e)
            null
        }
        
        val contextualQuery = buildContextualQuery(query, weatherContext, timeContext)
        logger.info("Built contextual query: $contextualQuery")
        
        // Convert enhanced recommendations to contextual format
        val contextualPlaces = enhancedResponse.recommendations.take(limit).mapNotNull { place ->
            try {
                // Parse coordinates from location string if needed
                val coords = parseLocationCoordinates(place.location)
                val placeLat = coords?.first ?: 37.5665 // Default Seoul coordinates
                val placeLon = coords?.second ?: 126.9780
                
                val distance = calculateDistance(lat, lon, placeLat, placeLon)
                
                com.mohe.spring.service.ContextualPlace(
                    id = place.id.toLongOrNull() ?: 0L,
                    name = place.title,
                    category = place.category,
                    description = place.description,
                    latitude = placeLat,
                    longitude = placeLon,
                    rating = place.rating ?: 0.0,
                    reviewCount = place.reviewCount,
                    imageUrl = place.image,
                    images = place.image?.let { listOf(it) } ?: emptyList(),
                    tags = place.tags,
                    operatingHours = null, // Not available in EnhancedPlaceRecommendation
                    distanceM = (distance * 1000).toInt(),
                    isOpenNow = true, // Assume open for now
                    score = place.recommendationScore.toDouble(),
                    reasonWhy = place.recommendationReasons.joinToString(", "),
                    weatherSuitability = weatherContext?.let { 
                        if (it.isRainy) "실내 활동 권장" else if (it.isComfortable) "야외 활동 적합" else "보통"
                    } ?: "정보 없음",
                    timeSuitability = getTimeSuitability(place.category, timeContext)
                )
            } catch (e: Exception) {
                logger.warn("Failed to convert enhanced recommendation to contextual place", e)
                null
            }
        }
        
        return ContextualRecommendationResponse(
            places = contextualPlaces,
            searchContext = com.mohe.spring.service.SearchContext(
                query = contextualQuery,
                weather = weatherContext?.weather?.conditionText ?: "날씨 정보 없음",
                time = timeContext,
                location = "위도 $lat, 경도 $lon",
                isAuthenticated = true,
                recommendationBasis = "MBTI, 북마크 히스토리, 날씨, 시간"
            ),
            totalResults = contextualPlaces.size
        )
    }
    
    private fun getGuestContextualRecommendations(
        lat: Double,
        lon: Double,
        query: String?,
        timeContext: String,
        limit: Int
    ): ContextualRecommendationResponse {
        logger.info("Getting guest recommendations based on popular places")
        
        // Get most bookmarked places (popular places)
        val popularPlaces = try {
            placeRepository.findRecommendablePlaces(PageRequest.of(0, limit * 2))
                .content
        } catch (e: Exception) {
            logger.warn("Failed to get popular places, using fallback", e)
            placeRepository.findTopRatedPlaces(3.0, PageRequest.of(0, limit * 2)).content
        }
        
        // Add weather context
        val weatherContext = try {
            weatherService.getWeatherContext(lat, lon)
        } catch (e: Exception) {
            logger.warn("Failed to get weather context for guest", e)
            null
        }
        
        val contextualQuery = buildContextualQuery(query, weatherContext, timeContext)
        logger.info("Built guest contextual query: $contextualQuery")
        
        // Filter and rank by weather/time suitability
        val suitablePlaces = popularPlaces.mapNotNull { place ->
            place.latitude?.let { placeLat ->
                place.longitude?.let { placeLon ->
                    val distance = calculateDistance(lat, lon, placeLat.toDouble(), placeLon.toDouble())
                    if (distance <= 20.0) { // Within 20km
                        val weatherScore = getWeatherSuitabilityScore(place.category, weatherContext)
                        val timeScore = getTimeSuitabilityScore(place.category, timeContext)
                        val combinedScore = (weatherScore + timeScore + (place.rating.toDouble() / 5.0)) / 3.0
                        
                        place to combinedScore
                    } else null
                }
            }
        }.sortedByDescending { it.second }
          .take(limit)
          .map { (place, score) ->
            com.mohe.spring.service.ContextualPlace(
                id = place.id!!,
                name = place.name ?: place.title ?: "",
                category = place.category,
                description = place.description,
                latitude = place.latitude?.toDouble(),
                longitude = place.longitude?.toDouble(),
                rating = place.rating.toDouble(),
                reviewCount = place.reviewCount,
                imageUrl = place.imageUrl,
                images = place.images,
                tags = place.tags,
                operatingHours = place.operatingHours,
                distanceM = (calculateDistance(lat, lon, place.latitude?.toDouble() ?: 0.0, place.longitude?.toDouble() ?: 0.0) * 1000).toInt(),
                isOpenNow = true, // Assume open for now
                score = score,
                reasonWhy = "인기 장소 (날씨·시간 기반 추천)",
                weatherSuitability = weatherContext?.let { 
                    if (it.isRainy) "실내 활동 권장" else if (it.isComfortable) "야외 활동 적합" else "보통"
                } ?: "정보 없음",
                timeSuitability = getTimeSuitability(place.category, timeContext)
            )
        }
        
        return ContextualRecommendationResponse(
            places = suitablePlaces,
            searchContext = com.mohe.spring.service.SearchContext(
                query = contextualQuery,
                weather = weatherContext?.weather?.conditionText ?: "날씨 정보 없음",
                time = timeContext,
                location = "위도 $lat, 경도 $lon",
                isAuthenticated = false,
                recommendationBasis = "인기 장소, 날씨, 시간"
            ),
            totalResults = suitablePlaces.size
        )
    }

    private fun getCurrentTimeContext(): String {
        val now = LocalTime.now()
        return when (now.hour) {
            in 6..11 -> "morning"
            in 12..17 -> "afternoon"
            in 18..22 -> "evening" 
            else -> "night"
        }
    }
    
    private fun buildContextualQuery(query: String?, weatherContext: Any?, timeContext: String): String {
        val parts = mutableListOf<String>()
        query?.let { parts.add(it) }
        
        when (timeContext) {
            "morning" -> parts.add("아침")
            "afternoon" -> parts.add("오후")
            "evening" -> parts.add("저녁")
            "night" -> parts.add("밤")
        }
        
        // Add weather context if available
        weatherContext?.let {
            try {
                val weatherData = it as com.mohe.spring.service.WeatherContext
                when {
                    weatherData.isRainy -> parts.add("비오는날")
                    weatherData.isHot -> parts.add("더운날")
                    weatherData.isCold -> parts.add("추운날")
                    weatherData.isComfortable -> parts.add("좋은날씨")
                    else -> {} // No specific weather context to add
                }
            } catch (e: Exception) {
                logger.debug("Could not extract weather context", e)
            }
        }
        
        return parts.joinToString(" ")
    }
    
    private fun getWeatherSuitabilityScore(category: String?, weatherContext: Any?): Double {
        if (weatherContext == null || category == null) return 0.5
        
        return try {
            val weather = weatherContext as com.mohe.spring.service.WeatherContext
            when {
                weather.isRainy && isIndoorCategory(category) -> 1.0
                weather.isComfortable && isOutdoorCategory(category) -> 1.0
                weather.isHot && isIndoorCategory(category) -> 0.8
                weather.isCold && isIndoorCategory(category) -> 0.8
                else -> 0.5
            }
        } catch (e: Exception) {
            0.5
        }
    }
    
    private fun getTimeSuitabilityScore(category: String?, timeContext: String): Double {
        if (category == null) return 0.5
        
        return when (timeContext) {
            "morning" -> when {
                category.contains("카페", true) || category.contains("베이커리", true) -> 1.0
                category.contains("공원", true) || category.contains("운동", true) -> 0.8
                else -> 0.5
            }
            "afternoon" -> when {
                category.contains("레스토랑", true) || category.contains("카페", true) -> 0.8
                category.contains("쇼핑", true) || category.contains("문화", true) -> 1.0
                else -> 0.6
            }
            "evening" -> when {
                category.contains("레스토랑", true) || category.contains("바", true) -> 1.0
                category.contains("문화", true) || category.contains("영화", true) -> 0.9
                else -> 0.5
            }
            "night" -> when {
                category.contains("바", true) || category.contains("클럽", true) -> 1.0
                category.contains("24시", true) -> 0.9
                else -> 0.3
            }
            else -> 0.5
        }
    }
    
    private fun getTimeSuitability(category: String?, timeContext: String): String {
        return when (getTimeSuitabilityScore(category, timeContext)) {
            in 0.8..1.0 -> "매우 적합"
            in 0.6..0.8 -> "적합"
            in 0.4..0.6 -> "보통"
            else -> "부적합"
        }
    }
    
    private fun isIndoorCategory(category: String): Boolean {
        val indoorKeywords = listOf("카페", "레스토랑", "쇼핑", "영화관", "도서관", "박물관", "미술관", "실내")
        return indoorKeywords.any { category.contains(it, true) }
    }
    
    private fun isOutdoorCategory(category: String): Boolean {
        val outdoorKeywords = listOf("공원", "등산", "해변", "운동장", "야외", "캠핑", "산책")
        return outdoorKeywords.any { category.contains(it, true) }
    }
    
    private fun parseLocationCoordinates(location: String): Pair<Double, Double>? {
        // Try to parse coordinates from location string
        // This is a simple implementation - could be enhanced based on your location format
        return try {
            val parts = location.split(",").map { it.trim() }
            if (parts.size >= 2) {
                val lat = parts[0].toDoubleOrNull()
                val lon = parts[1].toDoubleOrNull()
                if (lat != null && lon != null) {
                    Pair(lat, lon)
                } else null
            } else null
        } catch (e: Exception) {
            null
        }
    }
    
    private fun calculateDistance(lat1: Double, lon1: Double, lat2: Double, lon2: Double): Double {
        val earthRadius = 6371.0 // km
        val dLat = Math.toRadians(lat2 - lat1)
        val dLon = Math.toRadians(lon2 - lon1)
        val a = Math.sin(dLat / 2) * Math.sin(dLat / 2) +
                Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2)) *
                Math.sin(dLon / 2) * Math.sin(dLon / 2)
        val c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a))
        return earthRadius * c
    }

    private fun isValidMbtiType(mbti: String): Boolean {
        val validMbtiTypes = setOf(
            "INTJ", "INTP", "ENTJ", "ENTP",
            "INFJ", "INFP", "ENFJ", "ENFP",
            "ISTJ", "ISFJ", "ESTJ", "ESFJ",
            "ISTP", "ISFP", "ESTP", "ESFP"
        )
        return mbti.uppercase() in validMbtiTypes
    }

    /**
     * POST endpoint for recommendations query (legacy frontend compatibility)
     * Redirects to GET contextual endpoint
     */
    @Operation(
        summary = "Query recommendations (legacy)",
        description = "Legacy POST endpoint that redirects to contextual recommendations"
    )
    @ApiResponses(
        value = [
            SwaggerApiResponse(responseCode = "200", description = "Recommendations retrieved successfully"),
            SwaggerApiResponse(responseCode = "400", description = "Invalid parameters"),
            SwaggerApiResponse(responseCode = "500", description = "Failed to generate recommendations")
        ]
    )
    @PostMapping("/query")
    fun queryRecommendations(
        @RequestBody request: Map<String, Any?>,
        @AuthenticationPrincipal userPrincipal: UserPrincipal?
    ): ResponseEntity<ApiResponse<ContextualRecommendationResponse>> {
        return try {
            val lat = request["lat"]?.toString()?.toDoubleOrNull() ?: 37.5665
            val lon = request["lon"]?.toString()?.toDoubleOrNull() ?: 126.9780
            val query = request["query"]?.toString() ?: "좋은 장소"
            val limit = request["limit"]?.toString()?.toIntOrNull() ?: 10

            logger.info("POST query recommendations: lat=$lat, lon=$lon, query=$query, limit=$limit")

            // Redirect to contextual recommendations
            getContextualRecommendations(lat, lon, query, limit, userPrincipal)
        } catch (e: Exception) {
            logger.error("Failed to process query recommendations", e)
            ResponseEntity.status(500).body(
                ApiResponse.error("QUERY_ERROR", "Failed to process recommendations query: ${e.message}")
            )
        }
    }
}