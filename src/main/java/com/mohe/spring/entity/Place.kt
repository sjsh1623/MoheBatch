package com.mohe.spring.entity

import jakarta.persistence.*
import java.math.BigDecimal
import java.time.OffsetDateTime

@Entity
@Table(name = "places")
data class Place(
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    val id: Long = 0,
    
    @Column(nullable = false)
    val name: String,
    
    @Column(nullable = false)
    val title: String,
    
    val address: String? = null,
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
    
    @OneToMany(mappedBy = "place", cascade = [CascadeType.ALL], fetch = FetchType.LAZY)
    val bookmarks: List<Bookmark> = emptyList(),
    
    @OneToMany(mappedBy = "place", cascade = [CascadeType.ALL], fetch = FetchType.LAZY)
    val prompts: List<Prompt> = emptyList(),
    
    @OneToMany(mappedBy = "place", cascade = [CascadeType.ALL], fetch = FetchType.LAZY)
    val recentViews: List<RecentView> = emptyList()
)