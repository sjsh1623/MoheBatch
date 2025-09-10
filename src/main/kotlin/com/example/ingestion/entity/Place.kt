package com.example.ingestion.entity

import io.hypersistence.utils.hibernate.type.json.JsonType
import jakarta.persistence.*
import org.hibernate.annotations.JdbcTypeCode
import org.hibernate.annotations.Type
import org.hibernate.type.SqlTypes
import java.time.LocalDateTime

@Entity
@Table(name = "places")
data class Place(
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    val id: Long? = null,

    // Reverted fields
    @Column(name = "locat_name", nullable = false)
    var locatName: String,

    @Column(nullable = false)
    var sido: String,

    @Column(nullable = false)
    var sigungu: String,

    @Column(nullable = false)
    var dong: String,

    // Reverted to Double for compatibility with existing code
    var latitude: Double?,
    var longitude: Double?,

    @Type(JsonType::class)
    @Column(name = "naver_data", columnDefinition = "jsonb")
    var naverData: Map<String, Any>? = null,

    @Type(JsonType::class)
    @Column(name = "google_data", columnDefinition = "jsonb")
    var googleData: Map<String, Any>? = null,

    // New fields added
    @Column(nullable = false) // Assuming 'name' is a required field in DB
    var name: String, // This will be the 'name' for prompt generation

    var description: String?,

    @JdbcTypeCode(SqlTypes.ARRAY)
    @Column(columnDefinition = "text[]")
    var tags: List<String> = mutableListOf(),

    @Column(name = "image_url")
    var imageUrl: String? = null,

    @Column(name = "updated_at", nullable = false, updatable = false, insertable = false)
    var updatedAt: LocalDateTime? = null
)
