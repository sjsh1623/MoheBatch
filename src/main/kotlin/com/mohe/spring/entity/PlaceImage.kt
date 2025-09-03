package com.mohe.spring.entity

import jakarta.persistence.*
import java.time.OffsetDateTime

@Entity
@Table(
    name = "place_images",
    indexes = [
        Index(name = "idx_place_image_place_id", columnList = "place_id"),
        Index(name = "idx_place_image_is_primary", columnList = "is_primary"),
        Index(name = "idx_place_image_display_order", columnList = "display_order")
    ]
)
data class PlaceImage(
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    val id: Long? = null,
    
    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "place_id", nullable = false)
    val place: Place,
    
    @Column(name = "image_url", nullable = false, length = 2048)
    val imageUrl: String,
    
    @Column(name = "image_type")
    @Enumerated(EnumType.STRING)
    val imageType: ImageType = ImageType.GENERAL,
    
    @Column(name = "is_primary")
    val isPrimary: Boolean = false,
    
    @Column(name = "display_order")
    val displayOrder: Int = 0,
    
    @Column(name = "source")
    @Enumerated(EnumType.STRING)
    val source: ImageSource = ImageSource.GOOGLE_IMAGES,
    
    @Column(name = "source_id")
    val sourceId: String? = null, // Original ID from source API
    
    @Column(name = "width")
    val width: Int? = null,
    
    @Column(name = "height")
    val height: Int? = null,
    
    @Column(name = "file_size")
    val fileSize: Long? = null, // in bytes
    
    @Column(name = "alt_text")
    val altText: String? = null,
    
    @Column(name = "caption")
    val caption: String? = null,
    
    @Column(name = "is_verified")
    val isVerified: Boolean = false, // Manual verification flag
    
    @Column(name = "created_at")
    val createdAt: OffsetDateTime = OffsetDateTime.now(),
    
    @Column(name = "updated_at")
    val updatedAt: OffsetDateTime = OffsetDateTime.now()
)

enum class ImageType {
    GENERAL,        // General place photos
    EXTERIOR,       // Building exterior
    INTERIOR,       // Interior shots
    FOOD,          // Food photos (for restaurants)
    MENU,          // Menu photos
    AMBIANCE,      // Atmosphere/mood photos
    DETAIL,        // Close-up detail shots
    PANORAMIC      // Wide angle shots
}

enum class ImageSource {
    GOOGLE_IMAGES,     // Google Images API
    GOOGLE_PLACES,     // Google Places Photos API
    NAVER,             // Naver API
    MANUAL_UPLOAD,     // Admin uploaded
    WEB_SCRAPING       // Web scraped (if allowed)
}