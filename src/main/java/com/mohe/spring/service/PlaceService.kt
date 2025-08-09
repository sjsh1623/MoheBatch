package com.mohe.spring.service

import com.mohe.spring.dto.*
import com.mohe.spring.entity.Place
import com.mohe.spring.entity.RecentView
import com.mohe.spring.entity.User
import com.mohe.spring.repository.BookmarkRepository
import com.mohe.spring.repository.PlaceRepository
import com.mohe.spring.repository.RecentViewRepository
import com.mohe.spring.repository.UserRepository
import com.mohe.spring.security.UserPrincipal
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
    private val userRepository: UserRepository
) {
    
    fun getRecommendations(): PlaceRecommendationsResponse {
        val currentUser = getCurrentUser()
        val bookmarkedPlaceIds = bookmarkRepository.findBookmarkedPlaceIdsByUserId(currentUser.id)
        
        // Simple recommendation based on rating and popularity
        val pageable = PageRequest.of(0, 15, Sort.by(Sort.Direction.DESC, "rating", "popularity"))
        val places = placeRepository.findTopRatedPlaces(4.0, pageable)
        
        val recommendations = places.content.map { place ->
            PlaceRecommendationData(
                id = place.id.toString(),
                title = place.title,
                rating = place.rating,
                reviewCount = place.reviewCount,
                location = place.location,
                image = place.imageUrl,
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
        val currentUser = getCurrentUser()
        val bookmarkedPlaceIds = bookmarkRepository.findBookmarkedPlaceIdsByUserId(currentUser.id)
        
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
            PlaceCardData(
                id = place.id.toString(),
                title = place.title,
                rating = place.rating,
                reviewCount = place.reviewCount,
                location = place.location,
                image = place.imageUrl,
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
        
        return PlaceDetailResponse(
            place = PlaceDetailData(
                id = place.id.toString(),
                title = place.title,
                tags = place.tags,
                location = place.location,
                address = place.address,
                rating = place.rating,
                reviewCount = place.reviewCount,
                description = place.description,
                images = place.images,
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
            PlaceSearchResult(
                id = place.id.toString(),
                name = place.name,
                hours = place.operatingHours,
                location = place.location,
                rating = place.rating,
                carTime = place.transportationCarTime,
                busTime = place.transportationBusTime,
                tags = place.tags,
                image = place.imageUrl,
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
        val existingView = recentViewRepository.findByUserIdAndPlaceId(user.id, place.id)
        
        if (existingView != null) {
            recentViewRepository.updateViewedAt(user.id, place.id, OffsetDateTime.now())
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
    
    private fun getCurrentUser(): User {
        val userPrincipal = SecurityContextHolder.getContext().authentication.principal as UserPrincipal
        return userRepository.findById(userPrincipal.id)
            .orElseThrow { RuntimeException("사용자를 찾을 수 없습니다") }
    }
}