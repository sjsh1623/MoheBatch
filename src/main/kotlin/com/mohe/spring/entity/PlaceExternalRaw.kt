package com.mohe.spring.entity

import com.fasterxml.jackson.databind.JsonNode
import com.vladmihalcea.hibernate.type.json.JsonType
import jakarta.persistence.*
import org.hibernate.annotations.Type
import java.time.LocalDateTime

@Entity
@Table(
    name = "place_external_raw",
    indexes = [
        Index(name = "idx_place_external_raw_place_id", columnList = "place_id"),
        Index(name = "idx_place_external_raw_source", columnList = "source"),
        Index(name = "idx_place_external_raw_fetched_at", columnList = "fetched_at")
    ],
    uniqueConstraints = [UniqueConstraint(columnNames = ["source", "external_id"])]
)
data class PlaceExternalRaw(
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    val id: Long? = null,
    
    @Column(nullable = false, length = 50)
    val source: String, // 'naver', 'google', etc.
    
    @Column(name = "external_id", nullable = false, length = 255)
    val externalId: String,
    
    @Column(name = "place_id")
    val placeId: Long?, // FK to places table (nullable for failed processing)
    
    @Type(JsonType::class)
    @Column(nullable = false, columnDefinition = "jsonb")
    val payload: JsonNode,
    
    @Column(name = "fetched_at")
    val fetchedAt: LocalDateTime = LocalDateTime.now(),
    
    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "place_id", insertable = false, updatable = false)
    val place: Place? = null
)

enum class ExternalDataSource(val sourceName: String) {
    NAVER("naver"),
    GOOGLE("google"),
    FOURSQUARE("foursquare"),
    KAKAO("kakao");
    
    companion object {
        fun fromString(source: String): ExternalDataSource? = 
            values().find { it.sourceName.equals(source, ignoreCase = true) }
    }
}