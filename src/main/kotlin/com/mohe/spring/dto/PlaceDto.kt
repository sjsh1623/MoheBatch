package com.mohe.spring.dto

import java.math.BigDecimal
import java.time.OffsetDateTime

// Place List and Recommendations
data class PlaceListResponse(
    val places: List<PlaceCardData>,
    val pagination: PaginationData? = null
)

data class PlaceRecommendationsResponse(
    val recommendations: List<PlaceRecommendationData>,
    val totalCount: Int
)

data class PlaceCardData(
    val id: String,
    val title: String,
    val rating: BigDecimal,
    val reviewCount: Int? = null,
    val location: String?,
    val image: String?,
    val isBookmarked: Boolean
)

data class PlaceRecommendationData(
    val id: String,
    val title: String,
    val rating: BigDecimal,
    val reviewCount: Int,
    val location: String?,
    val image: String?,
    val tags: List<String>,
    val description: String?,
    val transportation: TransportationInfo?,
    val isBookmarked: Boolean,
    val recommendationReason: String?
)

data class TransportationInfo(
    val car: String?,
    val bus: String?
)

data class PaginationData(
    val currentPage: Int,
    val totalPages: Int,
    val totalCount: Int
)

// Place Detail
data class PlaceDetailResponse(
    val place: PlaceDetailData
)

data class PlaceDetailData(
    val id: String,
    val title: String,
    val tags: List<String>,
    val location: String?,
    val address: String?,
    val rating: BigDecimal,
    val reviewCount: Int,
    val description: String?,
    val images: List<String>,
    val gallery: List<String>,
    val additionalImageCount: Int,
    val transportation: TransportationInfo?,
    val operatingHours: String?,
    val amenities: List<String>,
    val isBookmarked: Boolean
)

// Search
data class PlaceSearchResponse(
    val searchResults: List<PlaceSearchResult>,
    val searchContext: SearchContext?
)

data class PlaceSearchResult(
    val id: String,
    val name: String,
    val hours: String?,
    val location: String?,
    val rating: BigDecimal,
    val carTime: String?,
    val busTime: String?,
    val tags: List<String>,
    val image: String?,
    val isBookmarked: Boolean,
    val weatherTag: TagInfo? = null,
    val noiseTag: TagInfo? = null
)

data class TagInfo(
    val text: String,
    val color: String,
    val icon: String
)

data class SearchContext(
    val weather: String?,
    val time: String?,
    val recommendation: String?
)