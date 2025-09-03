package com.mohe.spring.dto

import java.math.BigDecimal
import java.math.RoundingMode
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
    val rating: Double?, // Changed to Double for proper formatting
    val reviewCount: Int? = null,
    val location: String?,
    val image: String?,
    val images: List<PlaceImageData> = emptyList(), // Added images list
    val isBookmarked: Boolean
) {
    // Helper property to get formatted rating with one decimal place
    val formattedRating: String?
        get() = rating?.let { "%.1f".format(it) }
}

data class PlaceRecommendationData(
    val id: String,
    val title: String,
    val rating: Double?, // Changed to Double for proper formatting
    val reviewCount: Int,
    val location: String?,
    val image: String?,
    val images: List<PlaceImageData> = emptyList(), // Added images list
    val tags: List<String>,
    val description: String?,
    val transportation: TransportationInfo?,
    val isBookmarked: Boolean,
    val recommendationReason: String?
) {
    // Helper property to get formatted rating with one decimal place
    val formattedRating: String?
        get() = rating?.let { "%.1f".format(it) }
}

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

// Helper functions to convert between BigDecimal and Double
fun BigDecimal?.toFormattedDouble(): Double? {
    return this?.setScale(1, RoundingMode.HALF_UP)?.toDouble()
}

fun Double?.toBigDecimal(): BigDecimal {
    return this?.let { BigDecimal(it).setScale(1, RoundingMode.HALF_UP) } ?: BigDecimal.ZERO
}

data class PlaceDetailData(
    val id: String,
    val title: String,
    val tags: List<String>,
    val location: String?,
    val address: String?,
    val rating: Double?, // Changed to Double for proper formatting
    val reviewCount: Int,
    val description: String?,
    val images: List<PlaceImageData>, // Changed to proper image data
    val gallery: List<String>, // Keep for backward compatibility
    val additionalImageCount: Int,
    val transportation: TransportationInfo?,
    val operatingHours: String?,
    val amenities: List<String>,
    val isBookmarked: Boolean
) {
    // Helper property to get formatted rating with one decimal place
    val formattedRating: String?
        get() = rating?.let { "%.1f".format(it) }
}

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
    val rating: Double?, // Changed to Double for proper formatting
    val carTime: String?,
    val busTime: String?,
    val tags: List<String>,
    val image: String?,
    val images: List<PlaceImageData> = emptyList(), // Added images list
    val isBookmarked: Boolean,
    val weatherTag: TagInfo? = null,
    val noiseTag: TagInfo? = null
) {
    // Helper property to get formatted rating with one decimal place
    val formattedRating: String?
        get() = rating?.let { "%.1f".format(it) }
}

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

// New data class for place images
data class PlaceImageData(
    val id: Long,
    val imageUrl: String,
    val imageType: String,
    val isPrimary: Boolean,
    val displayOrder: Int,
    val source: String,
    val width: Int? = null,
    val height: Int? = null,
    val altText: String? = null,
    val caption: String? = null
)

// New data class for "지금 이 시간의 장소" response
data class CurrentTimeRecommendationsResponse(
    val places: List<PlaceCardData>,
    val context: CurrentTimeContext
)

data class CurrentTimeContext(
    val timeOfDay: String, // "morning", "afternoon", "evening", "night"
    val weatherCondition: String?, // "sunny", "rainy", "cloudy", etc.
    val recommendationMessage: String
)