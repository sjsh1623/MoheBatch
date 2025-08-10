package com.mohe.spring.entity

import com.fasterxml.jackson.databind.JsonNode
import com.vladmihalcea.hibernate.type.json.JsonType
import jakarta.persistence.*
import org.hibernate.annotations.Type
import java.math.BigDecimal
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.OffsetDateTime

@Entity
@Table(name = "places")
data class Place(
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    val id: Long? = null,
    
    @Column(nullable = false)
    val name: String,
    
    @Column(nullable = false)
    val title: String = name, // For backward compatibility
    
    val address: String? = null,
    
    @Column(name = "road_address")
    val roadAddress: String? = null,
    
    val location: String? = null,
    
    @Column(precision = 10, scale = 8)
    val latitude: BigDecimal? = null,
    
    @Column(precision = 11, scale = 8)
    val longitude: BigDecimal? = null,
    
    @Column(precision = 10, scale = 2)
    val altitude: BigDecimal? = null,
    
    val category: String? = null,
    val description: String? = null,
    
    @Column(name = "image_url")
    val imageUrl: String? = null,
    
    @Convert(converter = StringArrayConverter::class)
    val images: List<String> = emptyList(),
    
    @Convert(converter = StringArrayConverter::class)
    val gallery: List<String> = emptyList(),
    
    @Column(name = "additional_image_count")
    val additionalImageCount: Int = 0,
    
    @Column(precision = 3, scale = 2)
    val rating: BigDecimal = BigDecimal.ZERO,
    
    @Column(name = "review_count")
    val reviewCount: Int = 0,
    
    @Column(name = "operating_hours")
    val operatingHours: String? = null,
    
    @Convert(converter = StringArrayConverter::class)
    val amenities: List<String> = emptyList(),
    
    @Convert(converter = StringArrayConverter::class)
    val tags: List<String> = emptyList(),
    
    @Column(name = "transportation_car_time")
    val transportationCarTime: String? = null,
    
    @Column(name = "transportation_bus_time")
    val transportationBusTime: String? = null,
    
    @Convert(converter = StringArrayConverter::class)
    @Column(name = "weather_tags")
    val weatherTags: List<String> = emptyList(),
    
    @Convert(converter = StringArrayConverter::class)
    @Column(name = "noise_tags")
    val noiseTags: List<String> = emptyList(),
    
    val popularity: Int = 0,
    
    @Column(name = "created_at")
    val createdAt: OffsetDateTime = OffsetDateTime.now(),
    
    // New enhanced fields
    @Column(name = "naver_place_id", unique = true)
    val naverPlaceId: String? = null,
    
    @Column(name = "google_place_id", unique = true)
    val googlePlaceId: String? = null,
    
    val phone: String? = null,
    
    @Column(name = "website_url")
    val websiteUrl: String? = null,
    
    @Type(JsonType::class)
    @Column(name = "opening_hours", columnDefinition = "jsonb")
    val openingHours: JsonNode? = null,
    
    @Column(name = "types", columnDefinition = "text[]")
    val types: Array<String>? = null,
    
    @Column(name = "user_ratings_total")
    val userRatingsTotal: Int? = null,
    
    @Column(name = "price_level")
    val priceLevel: Short? = null,
    
    @Type(JsonType::class)
    @Column(name = "source_flags", columnDefinition = "jsonb")
    val sourceFlags: JsonNode? = null,
    
    @Column(name = "updated_at")
    val updatedAt: LocalDateTime = LocalDateTime.now(),
    
    // Age tracking fields
    @Column(name = "opened_date")
    val openedDate: LocalDate? = null,
    
    @Column(name = "first_seen_at")
    val firstSeenAt: OffsetDateTime = OffsetDateTime.now(),
    
    @Column(name = "last_rating_check")
    val lastRatingCheck: OffsetDateTime? = null,
    
    @Column(name = "is_new_place")
    val isNewPlace: Boolean = true,
    
    @Column(name = "should_recheck_rating")
    val shouldRecheckRating: Boolean = false,
    
    // Relationships
    @OneToMany(mappedBy = "place", cascade = [CascadeType.ALL], fetch = FetchType.LAZY)
    val bookmarks: List<Bookmark> = emptyList(),
    
    @OneToMany(mappedBy = "place", cascade = [CascadeType.ALL], fetch = FetchType.LAZY)
    val prompts: List<Prompt> = emptyList(),
    
    @OneToMany(mappedBy = "place", cascade = [CascadeType.ALL], fetch = FetchType.LAZY)
    val recentViews: List<RecentView> = emptyList()
) {
    // Helper method to get types as List<String> instead of Array<String>
    fun getTypesList(): List<String> = types?.toList() ?: emptyList()
    
    // Helper methods for age-based filtering
    fun isOlderThanSixMonths(): Boolean {
        return firstSeenAt.isBefore(OffsetDateTime.now().minusMonths(6))
    }
    
    fun isRecentlyOpened(): Boolean {
        return openedDate?.isAfter(LocalDate.now().minusMonths(6)) ?: false
    }
    
    fun shouldBeRecommended(): Boolean {
        // Recommend if rating >= 3.0 OR if it's a new place (< 6 months old)
        val hasGoodRating = rating.toDouble() >= 3.0
        val isNew = isNewPlace || isRecentlyOpened()
        return hasGoodRating || isNew
    }
    
    fun needsRatingRecheck(): Boolean {
        return shouldRecheckRating && isOlderThanSixMonths()
    }
}