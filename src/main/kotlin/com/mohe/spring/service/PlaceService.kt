package com.mohe.spring.service

import com.mohe.spring.dto.*
import com.mohe.spring.entity.Place
import com.mohe.spring.entity.PlaceImage
import com.mohe.spring.entity.RecentView
import com.mohe.spring.entity.User
import com.mohe.spring.repository.BookmarkRepository
import com.mohe.spring.repository.PlaceRepository
import com.mohe.spring.repository.PlaceImageRepository
import com.mohe.spring.repository.RecentViewRepository
import com.mohe.spring.repository.UserRepository
import com.mohe.spring.security.UserPrincipal
import org.slf4j.LoggerFactory
import org.springframework.data.domain.PageRequest
import org.springframework.data.domain.Pageable
import org.springframework.data.domain.Sort
import org.springframework.security.core.context.SecurityContextHolder
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional
import java.time.OffsetDateTime

@Service
@Transactional
class PlaceService(
    private val placeRepository: PlaceRepository,
    private val bookmarkRepository: BookmarkRepository,
    private val recentViewRepository: RecentViewRepository,
    private val userRepository: UserRepository,
    private val placeImageRepository: PlaceImageRepository,
    private val enhancedRecommendationService: EnhancedRecommendationService
) {
    private val logger = LoggerFactory.getLogger(PlaceService::class.java)
    
    fun getRecommendations(): PlaceRecommendationsResponse {
        val currentUser = getCurrentUser()
        
        try {
            // Use enhanced recommendation service with MBTI-weighted similarities
            val enhancedResponse = enhancedRecommendationService.getEnhancedRecommendations(
                user = currentUser,
                limit = 15,
                excludeBookmarked = true
            )
            
            // Convert enhanced recommendations to the expected response format
            val recommendations = enhancedResponse.recommendations.map { enhanced ->
                val placeImages = getPlaceImages(enhanced.id.toLong())
                PlaceRecommendationData(
                    id = enhanced.id,
                    title = enhanced.title,
                    rating = enhanced.rating?.toDouble(),
                    reviewCount = enhanced.reviewCount,
                    location = enhanced.location,
                    image = enhanced.image,
                    images = placeImages,
                    tags = enhanced.tags,
                    description = enhanced.mbtiDescription ?: enhanced.description, // Prefer MBTI description
                    transportation = enhanced.transportation,
                    isBookmarked = enhanced.isBookmarked,
                    recommendationReason = enhanced.recommendationReasons.firstOrNull() 
                        ?: generateMbtiRecommendationReason(currentUser, enhanced)
                )
            }
            
            return PlaceRecommendationsResponse(
                recommendations = recommendations,
                totalCount = recommendations.size
            )
            
        } catch (ex: Exception) {
            // Fallback to simple recommendation if enhanced service fails
            return getSimpleRecommendations(currentUser)
        }
    }
    
    /**
     * Fallback to simple recommendations if enhanced service fails
     */
    private fun getSimpleRecommendations(currentUser: User): PlaceRecommendationsResponse {
        val bookmarkedPlaceIds = bookmarkRepository.findBookmarkedPlaceIdsByUserId(currentUser.id)
        
        val pageable = PageRequest.of(0, 15, Sort.by(Sort.Direction.DESC, "rating", "popularity"))
        val places = placeRepository.findTopRatedPlaces(4.0, pageable)
        
        val recommendations = places.content.map { place ->
            val placeImages = getPlaceImages(place.id!!)
            
            PlaceRecommendationData(
                id = place.id.toString(),
                title = place.title,
                rating = place.rating.toFormattedDouble(),
                reviewCount = place.reviewCount,
                location = place.location,
                image = getBestAvailableImage(place),
                images = placeImages,
                tags = place.tags,
                description = place.description,
                transportation = TransportationInfo(
                    car = place.transportationCarTime,
                    bus = place.transportationBusTime
                ),
                isBookmarked = bookmarkedPlaceIds.contains(place.id),
                recommendationReason = generateRecommendationReason(currentUser, place)
            )
        }
        
        return PlaceRecommendationsResponse(
            recommendations = recommendations,
            totalCount = recommendations.size
        )
    }
    
    fun getPlaces(page: Int, limit: Int, category: String?, sort: String?): PlaceListResponse {
        val authentication = SecurityContextHolder.getContext().authentication
        val isAuthenticated = authentication != null && authentication.principal is UserPrincipal
        
        val bookmarkedPlaceIds = if (isAuthenticated) {
            try {
                val userPrincipal = authentication!!.principal as UserPrincipal
                val currentUser = userRepository.findById(userPrincipal.id)
                    .orElseThrow { RuntimeException("사용자를 찾을 수 없습니다") }
                bookmarkRepository.findBookmarkedPlaceIdsByUserId(currentUser.id)
            } catch (e: Exception) {
                emptySet() // Fallback to guest behavior if user loading fails
            }
        } else {
            emptySet()
        }
        
        val sortBy = when (sort) {
            "rating" -> Sort.by(Sort.Direction.DESC, "rating")
            "popularity" -> Sort.by(Sort.Direction.DESC, "popularity")
            else -> Sort.by(Sort.Direction.DESC, "createdAt")
        }
        
        val pageable = PageRequest.of(page - 1, limit, sortBy)
        val places = if (category != null) {
            placeRepository.findByCategory(category, pageable)
        } else {
            placeRepository.findAll(pageable)
        }
        
        val placeCards = places.content.map { place ->
            val placeImages = getPlaceImages(place.id!!)
            
            PlaceCardData(
                id = place.id.toString(),
                title = place.title,
                rating = place.rating.toFormattedDouble(),
                reviewCount = place.reviewCount,
                location = place.location,
                image = getBestAvailableImage(place),
                images = placeImages,
                isBookmarked = bookmarkedPlaceIds.contains(place.id)
            )
        }
        
        return PlaceListResponse(
            places = placeCards,
            pagination = PaginationData(
                currentPage = page,
                totalPages = places.totalPages,
                totalCount = places.totalElements.toInt()
            )
        )
    }
    
    fun getPlaceDetail(id: String): PlaceDetailResponse {
        val currentUser = getCurrentUser()
        val placeId = id.toLong()
        
        val place = placeRepository.findById(placeId)
            .orElseThrow { RuntimeException("장소를 찾을 수 없습니다") }
        
        // Record recent view
        recordRecentView(currentUser, place)
        
        val isBookmarked = bookmarkRepository.existsByUserAndPlace(currentUser, place)
        val placeImages = getPlaceImages(place.id!!)
        
        return PlaceDetailResponse(
            place = PlaceDetailData(
                id = place.id.toString(),
                title = place.title,
                tags = place.tags,
                location = place.location,
                address = place.address,
                rating = place.rating.toFormattedDouble(),
                reviewCount = place.reviewCount,
                description = place.description,
                images = placeImages,
                gallery = place.gallery,
                additionalImageCount = place.additionalImageCount,
                transportation = TransportationInfo(
                    car = place.transportationCarTime,
                    bus = place.transportationBusTime
                ),
                operatingHours = place.operatingHours,
                amenities = place.amenities,
                isBookmarked = isBookmarked
            )
        )
    }
    
    fun searchPlaces(
        query: String,
        location: String?,
        weather: String?,
        time: String?
    ): PlaceSearchResponse {
        val currentUser = getCurrentUser()
        val bookmarkedPlaceIds = bookmarkRepository.findBookmarkedPlaceIdsByUserId(currentUser.id)
        
        val pageable = PageRequest.of(0, 20, Sort.by(Sort.Direction.DESC, "rating"))
        val places = placeRepository.searchPlaces(query, pageable)
        
        val searchResults = places.content.map { place ->
            val placeImages = getPlaceImages(place.id!!)
            PlaceSearchResult(
                id = place.id.toString(),
                name = place.name,
                hours = place.operatingHours,
                location = place.location,
                rating = place.rating.toFormattedDouble(),
                carTime = place.transportationCarTime,
                busTime = place.transportationBusTime,
                tags = place.tags,
                image = getBestAvailableImage(place),
                images = placeImages,
                isBookmarked = bookmarkedPlaceIds.contains(place.id),
                weatherTag = generateWeatherTag(weather, place),
                noiseTag = generateNoiseTag(place)
            )
        }
        
        return PlaceSearchResponse(
            searchResults = searchResults,
            searchContext = SearchContext(
                weather = weather,
                time = time,
                recommendation = generateSearchRecommendation(weather, time)
            )
        )
    }
    
    private fun recordRecentView(user: User, place: Place) {
        val userId = user.id ?: return
        val placeId = place.id ?: return
        val existingView = recentViewRepository.findByUserIdAndPlaceId(userId, placeId)
        
        if (existingView != null) {
            recentViewRepository.updateViewedAt(userId, placeId, OffsetDateTime.now())
        } else {
            val recentView = RecentView(
                user = user,
                place = place,
                viewedAt = OffsetDateTime.now()
            )
            recentViewRepository.save(recentView)
        }
    }
    
    private fun generateRecommendationReason(user: User, place: Place): String {
        return when (user.mbti?.take(1)) {
            "E" -> "활기찬 분위기를 선호하는 ${user.mbti} 성향에 맞춤"
            "I" -> "조용한 공간을 선호하는 ${user.mbti} 성향에 맞춤"
            else -> "높은 평점의 인기 장소입니다"
        }
    }
    
    private fun generateMbtiRecommendationReason(user: User, enhanced: EnhancedPlaceRecommendation): String {
        return when {
            enhanced.mbtiDescription != null && user.mbti != null -> {
                "${user.mbti} 성향에 특별히 맞춤 추천"
            }
            enhanced.recommendationScore.toDouble() > 0.8 -> {
                "당신의 취향과 매우 유사한 장소"
            }
            enhanced.recommendationScore.toDouble() > 0.6 -> {
                "당신의 관심사와 비슷한 장소"
            }
            else -> generateRecommendationReason(user, 
                Place(
                    name = enhanced.title,
                    category = enhanced.category
                ))
        }
    }
    
    private fun generateWeatherTag(weather: String?, place: Place): TagInfo? {
        return when (weather) {
            "hot" -> TagInfo(
                text = "더운 날씨에 가기 좋은 장소",
                color = "red",
                icon = "thermometer"
            )
            "cold" -> TagInfo(
                text = "추운 날씨에 가기 좋은 장소",
                color = "blue",
                icon = "snowflake"
            )
            else -> null
        }
    }
    
    private fun generateNoiseTag(place: Place): TagInfo? {
        return if (place.tags.any { it.contains("조용") }) {
            TagInfo(
                text = "시끄럽지 않은 장소",
                color = "blue",
                icon = "speaker"
            )
        } else null
    }
    
    private fun generateSearchRecommendation(weather: String?, time: String?): String {
        val timeText = when (time) {
            "morning" -> "오전"
            "afternoon" -> "오후"
            "evening" -> "저녁"
            else -> "지금"
        }
        
        val weatherText = when (weather) {
            "hot" -> "더운 날씨"
            "cold" -> "추운 날씨"
            else -> "현재 날씨"
        }
        
        return "${timeText}에는 멀지 않고 ${weatherText}에 적합한 실내 장소들을 추천드릴게요."
    }

    fun getPopularPlaces(latitude: Double, longitude: Double): PlaceListResponse {
        return try {
            // For popular places, we don't need to check bookmarks for guest users
            // This endpoint should work for everyone without authentication
            val places = placeRepository.findPopularPlaces(latitude, longitude, 10000.0) // 10km radius

            val placeCards = places.map { place ->
                val placeImages = getPlaceImages(place.id!!)
                
                PlaceCardData(
                    id = place.id.toString(),
                    title = place.title,
                    rating = place.rating.toFormattedDouble(),
                    reviewCount = place.reviewCount,
                    location = place.location,
                    image = getBestAvailableImage(place),
                    images = placeImages,
                    isBookmarked = false // Popular places endpoint doesn't show bookmark status
                )
            }

            PlaceListResponse(
                places = placeCards,
                pagination = PaginationData(
                    currentPage = 1,
                    totalPages = 1,
                    totalCount = placeCards.size
                )
            )
        } catch (e: Exception) {
            // Return empty result on error
            PlaceListResponse(
                places = emptyList(),
                pagination = PaginationData(
                    currentPage = 1,
                    totalPages = 1,
                    totalCount = 0
                )
            )
        }
    }
    
    /**
     * Get place images from the place_images table
     */
    private fun getPlaceImages(placeId: Long): List<PlaceImageData> {
        return placeImageRepository.findByPlaceIdOrderByDisplayOrderAsc(placeId)
            .map { image ->
                PlaceImageData(
                    id = image.id!!,
                    imageUrl = image.imageUrl,
                    imageType = image.imageType.name,
                    isPrimary = image.isPrimary,
                    displayOrder = image.displayOrder,
                    source = image.source.name,
                    width = image.width,
                    height = image.height,
                    altText = image.altText,
                    caption = image.caption
                )
            }
    }
    
    /**
     * Get the best available image for a place
     * Priority: Primary image from place_images table -> place.imageUrl -> first image from images array
     */
    private fun getBestAvailableImage(place: Place): String? {
        // First try to get primary image from place_images table
        place.id?.let { placeId ->
            val primaryImage = placeImageRepository.findByPlaceIdAndIsPrimaryTrue(placeId)
            if (primaryImage != null) {
                return primaryImage.imageUrl
            }
            
            // If no primary, get first image from place_images table
            val firstImage = placeImageRepository.findByPlaceIdOrderByDisplayOrderAsc(placeId).firstOrNull()
            if (firstImage != null) {
                return firstImage.imageUrl
            }
        }
        
        // Fallback to existing logic
        return when {
            place.imageUrl?.isNotBlank() == true -> place.imageUrl
            place.images.isNotEmpty() -> place.images.firstOrNull { it.isNotBlank() }
            place.gallery.isNotEmpty() -> place.gallery.firstOrNull { it.isNotBlank() }
            else -> null
        }
    }
    
    /**
     * Debug method to check database state
     */
    fun getDebugInfo(): Map<String, Any> {
        return try {
            val totalPlaces = placeRepository.count()
            val placesWithImages = placeRepository.count() // Simplified for now
            
            // Sample places for debugging
            val samplePlaces = placeRepository.findAll(PageRequest.of(0, 5))
                .content
                .map { place ->
                    mapOf(
                        "id" to place.id,
                        "name" to place.name,
                        "category" to place.category,
                        "rating" to place.rating,
                        "location" to "${place.latitude}, ${place.longitude}",
                        "hasImages" to (place.imageUrl != null || place.images.isNotEmpty())
                    )
                }
            
            mapOf(
                "totalPlaces" to totalPlaces,
                "placesWithImages" to placesWithImages,
                "samplePlaces" to samplePlaces,
                "databaseStatus" to "connected"
            )
        } catch (e: Exception) {
            mapOf(
                "error" to "Debug info collection failed: ${e.message}",
                "databaseStatus" to "error"
            )
        }
    }
    
    private fun getCurrentUser(): User {
        val authentication = SecurityContextHolder.getContext().authentication
        if (authentication == null || authentication.principal !is UserPrincipal) {
            throw RuntimeException("인증이 필요합니다")
        }
        val userPrincipal = authentication.principal as UserPrincipal
        return userRepository.findById(userPrincipal.id)
            .orElseThrow { RuntimeException("사용자를 찾을 수 없습니다") }
    }

    /**
     * Get general places list with pagination and sorting
     */
    fun getPlacesList(page: Int, limit: Int, sort: String): PlaceListResponse {
        logger.info("getPlacesList called with page=$page, limit=$limit, sort=$sort")
        
        val safePage = if (page < 0) 0 else page
        val safeLimit = if (limit < 1) 10 else if (limit > 100) 100 else limit
        
        logger.info("Using safe values: safePage=$safePage, safeLimit=$safeLimit")
        
        val pageable = when (sort) {
            "rating" -> PageRequest.of(safePage, safeLimit, Sort.by(Sort.Direction.DESC, "rating"))
            "recent" -> PageRequest.of(safePage, safeLimit, Sort.by(Sort.Direction.DESC, "createdAt"))
            else -> PageRequest.of(safePage, safeLimit, Sort.by(Sort.Direction.DESC, "reviewCount")) // popularity
        }

        val placesPage = placeRepository.findAll(pageable)
        
        val placeCards = placesPage.content.map { place ->
            val placeImages = getPlaceImages(place.id!!)
            
            PlaceCardData(
                id = place.id.toString(),
                title = place.title,
                rating = place.rating.toFormattedDouble(),
                reviewCount = place.reviewCount,
                location = place.location,
                image = getBestAvailableImage(place),
                images = placeImages,
                isBookmarked = false // General endpoint doesn't show bookmark status for anonymous users
            )
        }

        return PlaceListResponse(
            places = placeCards,
            pagination = PaginationData(
                currentPage = placesPage.number + 1,
                totalPages = placesPage.totalPages,
                totalCount = placesPage.totalElements.toInt()
            )
        )
    }

    /**
     * Get current time recommendations for non-logged-in users
     * Based on time of day and weather conditions
     */
    fun getCurrentTimePlaces(latitude: Double?, longitude: Double?, limit: Int): CurrentTimeRecommendationsResponse {
        val currentHour = java.time.LocalTime.now().hour
        val timeOfDay = when {
            currentHour >= 5 && currentHour < 12 -> "morning"
            currentHour >= 12 && currentHour < 17 -> "afternoon" 
            currentHour >= 17 && currentHour < 22 -> "evening"
            else -> "night"
        }
        
        // Determine appropriate categories based on time of day
        val preferredCategories = when (timeOfDay) {
            "morning" -> listOf("카페", "베이커리", "맛집") // Morning: Cafes, bakeries, breakfast spots
            "afternoon" -> listOf("카페", "맛집", "공원", "박물관") // Afternoon: Cafes, restaurants, parks, museums
            "evening" -> listOf("맛집", "카페", "술집", "바") // Evening: Restaurants, cafes, bars
            else -> listOf("맛집", "카페", "술집", "바", "노래방") // Night: Late night venues
        }
        
        // Build query based on categories and location
        val pageable = PageRequest.of(0, limit, Sort.by(Sort.Direction.DESC, "rating", "reviewCount"))
        val places = if (latitude != null && longitude != null) {
            // Get places near location that match time preferences
            placeRepository.findNearbyPlacesByTimePreference(
                latitude.toBigDecimal(),
                longitude.toBigDecimal(),
                preferredCategories.toTypedArray(),
                10000.0, // 10km radius
                pageable
            )
        } else {
            // Get general places that match time preferences
            placeRepository.findPlacesByTimePreference(preferredCategories, pageable)
        }
        
        val placeCards = places.map { place ->
            val placeImages = getPlaceImages(place.id!!)
            
            PlaceCardData(
                id = place.id.toString(),
                title = place.title,
                rating = place.rating.toFormattedDouble(),
                reviewCount = place.reviewCount,
                location = place.location,
                image = getBestAvailableImage(place),
                images = placeImages,
                isBookmarked = false // Current time endpoint doesn't show bookmark status
            )
        }
        
        val recommendationMessage = generateTimeBasedRecommendation(timeOfDay)
        val context = CurrentTimeContext(
            timeOfDay = timeOfDay,
            weatherCondition = null, // Can be enhanced with weather service integration
            recommendationMessage = recommendationMessage
        )
        
        return CurrentTimeRecommendationsResponse(
            places = placeCards,
            context = context
        )
    }
    
    /**
     * Generate recommendation message based on time of day
     */
    private fun generateTimeBasedRecommendation(timeOfDay: String): String {
        return when (timeOfDay) {
            "morning" -> "상쾌한 아침을 시작할 수 있는 장소를 추천드려요."
            "afternoon" -> "오후 시간을 즐기기에 좋은 장소들입니다."
            "evening" -> "저녁 시간에 어울리는 분위기 좋은 곳을 찾아드렸어요."
            else -> "밤 시간에도 즉길 수 있는 늦은 시간까지 열려 있는 곳들입니다."
        }
    }
}