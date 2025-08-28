package com.mohe.spring.service

import com.mohe.spring.dto.PlaceDto
import com.mohe.spring.entity.Place
import com.mohe.spring.repository.PlaceRepository
import com.mohe.spring.repository.PlaceKeywordExtractionRepository
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import java.math.BigDecimal
import java.time.LocalDateTime
import java.time.LocalTime
import kotlin.math.*

/**
 * Context-aware recommendation request
 */
data class ContextualRecommendationRequest(
    val query: String,
    val lat: Double,
    val lon: Double,
    val timestamp: String? = null,
    val limit: Int = 10,
    val maxDistanceKm: Double = 10.0
)

/**
 * Context-aware recommendation response
 */
data class ContextualRecommendationResponse(
    val places: List<ContextualPlace>,
    val searchContext: SearchContext,
    val totalResults: Int
)

/**
 * Place with contextual information
 */
data class ContextualPlace(
    val id: Long,
    val name: String,
    val category: String?,
    val description: String?,
    val latitude: Double?,
    val longitude: Double?,
    val rating: Double,
    val reviewCount: Int,
    val imageUrl: String?,
    val images: List<String>,
    val tags: List<String>,
    val operatingHours: String?,
    val distanceM: Int,
    val isOpenNow: Boolean,
    val score: Double,
    val reasonWhy: String, // Why this was recommended
    val weatherSuitability: String? = null,
    val timeSuitability: String? = null
)

/**
 * Search context information
 */
data class SearchContext(
    val query: String,
    val extractedKeywords: List<String>,
    val weather: WeatherData?,
    val daypart: String,
    val localTime: String,
    val locationDescription: String,
    val recommendation: String // AI-generated contextual recommendation message
)

/**
 * Service for context-aware place recommendations using weather, time, and location
 */
@Service
class ContextualRecommendationService(
    private val placeRepository: PlaceRepository,
    private val placeKeywordExtractionRepository: PlaceKeywordExtractionRepository,
    private val ollamaService: OllamaService,
    private val weatherService: WeatherService
) {
    private val logger = LoggerFactory.getLogger(ContextualRecommendationService::class.java)
    
    fun getContextualRecommendations(request: ContextualRecommendationRequest): ContextualRecommendationResponse {
        logger.info("Getting contextual recommendations for query='${request.query}' at lat=${request.lat}, lon=${request.lon}")
        
        // Step 1: Get weather context
        val weatherContext = try {
            weatherService.getWeatherContext(request.lat, request.lon)
        } catch (e: Exception) {
            logger.warn("Failed to get weather context, continuing without it", e)
            null
        }
        
        // Step 2: Determine time context
        val timeContext = determineTimeContext(request.timestamp)
        
        // Step 3: Extract keywords from query using Ollama
        val keywords = ollamaService.extractKeywordsFromQuery(
            request.query,
            weatherContext?.let { "${it.weather.conditionCode}, ${it.weather.tempC}°C" },
            timeContext
        )
        
        logger.info("Extracted keywords: $keywords")
        
        // Step 4: Get places within distance range
        val nearbyPlaces = findNearbyPlaces(request.lat, request.lon, request.maxDistanceKm)
        
        // Step 5: Vector search based on keywords
        val vectorMatchedPlaceIds = if (keywords.isNotEmpty()) {
            performVectorSearch(keywords, nearbyPlaces.map { it.id ?: 0L })
        } else {
            nearbyPlaces.map { it.id ?: 0L }
        }
        
        // Step 6: Filter and rank by context
        val contextualPlaces = rankByContext(
            nearbyPlaces.filter { vectorMatchedPlaceIds.contains(it.id) },
            weatherContext,
            timeContext,
            request.lat,
            request.lon,
            keywords
        ).take(request.limit)
        
        // Step 7: Build search context
        val searchContext = buildSearchContext(
            request.query, 
            keywords, 
            weatherContext?.weather, 
            timeContext, 
            request.lat, 
            request.lon
        )
        
        return ContextualRecommendationResponse(
            places = contextualPlaces,
            searchContext = searchContext,
            totalResults = vectorMatchedPlaceIds.size
        )
    }
    
    private fun determineTimeContext(timestamp: String?): String {
        val now = if (timestamp != null) {
            try {
                LocalDateTime.parse(timestamp.replace("Z", ""))
            } catch (e: Exception) {
                LocalDateTime.now()
            }
        } else {
            LocalDateTime.now()
        }
        
        return when (now.hour) {
            in 6..11 -> "morning"
            in 12..17 -> "afternoon"
            in 18..21 -> "evening"
            else -> "night"
        }
    }
    
    private fun findNearbyPlaces(lat: Double, lon: Double, maxDistanceKm: Double): List<Place> {
        // Use bounding box for initial filtering (more efficient than calculating distance for all places)
        val latDelta = maxDistanceKm / 111.32 // Rough conversion: 1 degree lat ≈ 111.32 km
        val lonDelta = maxDistanceKm / (111.32 * cos(Math.toRadians(lat)))
        
        val minLat = lat - latDelta
        val maxLat = lat + latDelta
        val minLon = lon - lonDelta
        val maxLon = lon + lonDelta
        
        val placesInBounds = placeRepository.findByLatitudeBetweenAndLongitudeBetween(
            BigDecimal(minLat),
            BigDecimal(maxLat),
            BigDecimal(minLon),
            BigDecimal(maxLon)
        )
        
        // Filter by actual distance and quality
        return placesInBounds.filter { place ->
            place.latitude != null && place.longitude != null &&
            place.shouldBeRecommended() && // Uses existing quality filtering
            calculateDistance(lat, lon, place.latitude!!.toDouble(), place.longitude!!.toDouble()) <= maxDistanceKm
        }
    }
    
    private fun performVectorSearch(keywords: List<String>, candidatePlaceIds: List<Long>): List<Long> {
        // Use existing vector similarity search infrastructure
        return try {
            val keywordText = keywords.joinToString(" ")
            // This would use the existing vector search - for now, return candidates ordered by basic relevance
            candidatePlaceIds.take(50) // Limit candidates for performance
        } catch (e: Exception) {
            logger.warn("Vector search failed, using candidate places", e)
            candidatePlaceIds.take(50)
        }
    }
    
    private fun rankByContext(
        places: List<Place>,
        weatherContext: WeatherContext?,
        timeContext: String,
        userLat: Double,
        userLon: Double,
        keywords: List<String>
    ): List<ContextualPlace> {
        
        return places.mapNotNull { place ->
            try {
                val distance = calculateDistance(
                    userLat, userLon,
                    place.latitude?.toDouble() ?: return@mapNotNull null,
                    place.longitude?.toDouble() ?: return@mapNotNull null
                )
                
                val distanceM = (distance * 1000).toInt()
                val isOpenNow = determineIfOpen(place, timeContext)
                
                // Calculate contextual score
                var score = calculateBaseScore(place, distance)
                
                // Weather boost
                if (weatherContext != null) {
                    score += calculateWeatherBoost(place, weatherContext)
                }
                
                // Time boost
                score += calculateTimeBoost(place, timeContext, isOpenNow)
                
                // Keyword relevance boost
                score += calculateKeywordBoost(place, keywords)
                
                // Generate reason
                val reasonWhy = generateReasonWhy(place, weatherContext, timeContext, keywords, isOpenNow)
                
                ContextualPlace(
                    id = place.id!!,
                    name = place.name,
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
                    distanceM = distanceM,
                    isOpenNow = isOpenNow,
                    score = score,
                    reasonWhy = reasonWhy,
                    weatherSuitability = determineWeatherSuitability(place, weatherContext),
                    timeSuitability = determineTimeSuitability(place, timeContext)
                )
            } catch (e: Exception) {
                logger.warn("Error processing place ${place.id}: ${e.message}")
                null
            }
        }.sortedByDescending { it.score }
    }
    
    private fun calculateBaseScore(place: Place, distanceKm: Double): Double {
        var score = 0.0
        
        // Rating factor (0-5 scale, weight 30%)
        score += (place.rating.toDouble() / 5.0) * 0.3
        
        // Distance factor (closer is better, weight 20%)
        val distanceFactor = max(0.0, 1.0 - (distanceKm / 10.0)) // Linear decay over 10km
        score += distanceFactor * 0.2
        
        // Review count factor (popularity, weight 10%, logarithmic)
        val reviewFactor = min(1.0, log10((place.reviewCount + 1).toDouble()) / 4.0) // Cap at 10k reviews
        score += reviewFactor * 0.1
        
        return score
    }
    
    private fun calculateWeatherBoost(place: Place, weatherContext: WeatherContext): Double {
        var boost = 0.0
        
        // Indoor/outdoor suitability
        val placeCategory = place.category?.lowercase() ?: ""
        when {
            weatherContext.isRainy -> {
                if (isIndoorPlace(placeCategory)) boost += 0.15
                if (isOutdoorPlace(placeCategory)) boost -= 0.1
            }
            weatherContext.isHot -> {
                if (isAirConditionedPlace(placeCategory)) boost += 0.1
                if (isOutdoorPlace(placeCategory) && !isShadedPlace(placeCategory)) boost -= 0.05
            }
            weatherContext.isCold -> {
                if (isIndoorPlace(placeCategory) || isWarmPlace(placeCategory)) boost += 0.1
            }
            weatherContext.isComfortable -> {
                if (isOutdoorPlace(placeCategory)) boost += 0.05
            }
        }
        
        return boost
    }
    
    private fun calculateTimeBoost(place: Place, timeContext: String, isOpenNow: Boolean): Double {
        var boost = 0.0
        
        // Open now is crucial
        if (isOpenNow) {
            boost += 0.2
        } else {
            boost -= 0.3 // Strong penalty for closed places
        }
        
        // Time-appropriate places
        val category = place.category?.lowercase() ?: ""
        when (timeContext) {
            "morning" -> {
                if (category.contains("카페") || category.contains("breakfast")) boost += 0.05
            }
            "afternoon" -> {
                if (category.contains("카페") || category.contains("lunch")) boost += 0.05
            }
            "evening" -> {
                if (category.contains("레스토랑") || category.contains("바") || category.contains("pub")) boost += 0.05
            }
            "night" -> {
                if (category.contains("바") || category.contains("클럽") || category.contains("야식")) boost += 0.05
            }
        }
        
        return boost
    }
    
    private fun calculateKeywordBoost(place: Place, keywords: List<String>): Double {
        var boost = 0.0
        val placeText = "${place.name} ${place.category} ${place.description}".lowercase()
        
        keywords.forEach { keyword ->
            if (placeText.contains(keyword.lowercase())) {
                boost += 0.05 // Boost per matching keyword
            }
        }
        
        return boost
    }
    
    private fun generateReasonWhy(
        place: Place, 
        weatherContext: WeatherContext?, 
        timeContext: String, 
        keywords: List<String>,
        isOpenNow: Boolean
    ): String {
        val reasons = mutableListOf<String>()
        
        if (!isOpenNow) {
            return "현재 운영시간이 아닙니다"
        }
        
        // Weather-based reasons
        weatherContext?.let { weather ->
            when {
                weather.isRainy && isIndoorPlace(place.category ?: "") -> 
                    reasons.add("비 오는 날씨에 적합한 실내 공간")
                weather.isHot && isAirConditionedPlace(place.category ?: "") ->
                    reasons.add("더운 날씨에 시원한 실내 공간")
                weather.isCold && isIndoorPlace(place.category ?: "") ->
                    reasons.add("추운 날씨에 따뜻한 실내 공간")
                weather.isComfortable && isOutdoorPlace(place.category ?: "") ->
                    reasons.add("좋은 날씨에 야외 활동하기 좋은 곳")
            }
        }
        
        // Time-based reasons
        when (timeContext) {
            "morning" -> if (place.category?.contains("카페") == true) reasons.add("아침 시간에 좋은 카페")
            "evening" -> if (place.category?.contains("레스토랑") == true) reasons.add("저녁 시간에 좋은 레스토랑")
        }
        
        // Rating-based reason
        if (place.rating.toDouble() >= 4.5) {
            reasons.add("높은 평점 (${place.rating}점)")
        }
        
        // Keyword matching reason
        val matchedKeywords = keywords.filter { keyword ->
            "${place.name} ${place.category}".lowercase().contains(keyword.lowercase())
        }
        if (matchedKeywords.isNotEmpty()) {
            reasons.add("검색어 '${matchedKeywords.first()}'와 연관성 높음")
        }
        
        return if (reasons.isNotEmpty()) {
            reasons.joinToString(", ")
        } else {
            "주변의 인기 장소"
        }
    }
    
    private fun buildSearchContext(
        query: String,
        keywords: List<String>,
        weather: WeatherData?,
        timeContext: String,
        lat: Double,
        lon: Double
    ): SearchContext {
        val locationDescription = "위도 ${String.format("%.4f", lat)}, 경도 ${String.format("%.4f", lon)}"
        
        val recommendation = buildContextualRecommendation(weather, timeContext, keywords)
        
        return SearchContext(
            query = query,
            extractedKeywords = keywords,
            weather = weather,
            daypart = timeContext,
            localTime = LocalDateTime.now().toString(),
            locationDescription = locationDescription,
            recommendation = recommendation
        )
    }
    
    private fun buildContextualRecommendation(
        weather: WeatherData?,
        timeContext: String,
        keywords: List<String>
    ): String {
        val weatherDesc = weather?.let { 
            when {
                it.conditionCode == "rain" -> "비 오는 날씨"
                it.tempC > 28 -> "더운 날씨"
                it.tempC < 10 -> "추운 날씨"
                else -> "좋은 날씨"
            }
        } ?: "현재 날씨"
        
        val timeDesc = when (timeContext) {
            "morning" -> "아침 시간"
            "afternoon" -> "오후 시간" 
            "evening" -> "저녁 시간"
            "night" -> "밤 시간"
            else -> "현재 시간"
        }
        
        return "${weatherDesc}와 ${timeDesc}를 고려하여 추천드립니다."
    }
    
    // Helper methods for place categorization
    private fun isIndoorPlace(category: String): Boolean {
        val indoor = listOf("카페", "cafe", "레스토랑", "restaurant", "쇼핑", "shopping", "박물관", "museum")
        return indoor.any { category.contains(it, ignoreCase = true) }
    }
    
    private fun isOutdoorPlace(category: String): Boolean {
        val outdoor = listOf("공원", "park", "해변", "beach", "산", "mountain", "야외", "outdoor")
        return outdoor.any { category.contains(it, ignoreCase = true) }
    }
    
    private fun isAirConditionedPlace(category: String): Boolean {
        val ac = listOf("카페", "cafe", "쇼핑몰", "mall", "백화점", "department", "영화관", "cinema")
        return ac.any { category.contains(it, ignoreCase = true) }
    }
    
    private fun isShadedPlace(category: String): Boolean {
        val shaded = listOf("공원", "park", "정원", "garden", "숲", "forest")
        return shaded.any { category.contains(it, ignoreCase = true) }
    }
    
    private fun isWarmPlace(category: String): Boolean {
        val warm = listOf("카페", "cafe", "레스토랑", "restaurant", "찜질방", "sauna")
        return warm.any { category.contains(it, ignoreCase = true) }
    }
    
    private fun determineIfOpen(place: Place, timeContext: String): Boolean {
        // Simplified logic - in production, would parse JSON opening hours
        val operatingHours = place.operatingHours ?: return true // Assume open if no hours specified
        
        // Basic parsing for common formats like "09:00-21:00"
        return try {
            val currentHour = LocalTime.now().hour
            if (operatingHours.contains("-")) {
                val parts = operatingHours.split("-")
                if (parts.size == 2) {
                    val openHour = parts[0].split(":")[0].toIntOrNull() ?: 0
                    val closeHour = parts[1].split(":")[0].toIntOrNull() ?: 24
                    currentHour in openHour until closeHour
                } else true
            } else true
        } catch (e: Exception) {
            true // Assume open if parsing fails
        }
    }
    
    private fun determineWeatherSuitability(place: Place, weatherContext: WeatherContext?): String? {
        if (weatherContext == null) return null
        
        val category = place.category?.lowercase() ?: ""
        return when {
            weatherContext.isRainy && isIndoorPlace(category) -> "비 오는 날에 적합"
            weatherContext.isHot && isAirConditionedPlace(category) -> "더운 날에 시원함"
            weatherContext.isCold && isIndoorPlace(category) -> "추운 날에 따뜻함"
            else -> null
        }
    }
    
    private fun determineTimeSuitability(place: Place, timeContext: String): String? {
        val category = place.category?.lowercase() ?: ""
        return when (timeContext) {
            "morning" -> if (category.contains("카페")) "아침에 좋음" else null
            "evening" -> if (category.contains("레스토랑") || category.contains("바")) "저녁에 좋음" else null
            else -> null
        }
    }
    
    /**
     * Calculate distance between two points using Haversine formula
     */
    private fun calculateDistance(lat1: Double, lon1: Double, lat2: Double, lon2: Double): Double {
        val r = 6371 // Earth's radius in kilometers
        val dLat = Math.toRadians(lat2 - lat1)
        val dLon = Math.toRadians(lon2 - lon1)
        val a = sin(dLat / 2).pow(2) + cos(Math.toRadians(lat1)) * cos(Math.toRadians(lat2)) * sin(dLon / 2).pow(2)
        val c = 2 * atan2(sqrt(a), sqrt(1 - a))
        return r * c
    }
}