package com.example.ingestion.entity

import jakarta.persistence.*
import java.time.LocalDateTime
import java.time.OffsetDateTime

@Entity
@Table(
    name = "ingested_data",
    indexes = [
        Index(name = "idx_external_id", columnList = "external_id", unique = true),
        Index(name = "idx_email", columnList = "email"),
        Index(name = "idx_updated_at", columnList = "updated_at")
    ]
)
data class DataEntity(
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    val id: Long? = null,
    
    @Column(name = "external_id", nullable = false, unique = true, length = 100)
    val externalId: String,
    
    @Column(name = "name", nullable = false, length = 255)
    val name: String,
    
    @Column(name = "email", nullable = false, length = 255)
    val email: String,
    
    @Column(name = "phone", length = 50)
    val phone: String? = null,
    
    @Column(name = "department", length = 100)
    val department: String? = null,
    
    @Column(name = "status", length = 50)
    val status: String? = null,
    
    @Column(name = "external_created_at", nullable = false)
    val externalCreatedAt: OffsetDateTime,
    
    @Column(name = "external_updated_at", nullable = false) 
    val externalUpdatedAt: OffsetDateTime,
    
    @Column(name = "created_at", nullable = false)
    val createdAt: LocalDateTime = LocalDateTime.now(),
    
    @Column(name = "updated_at", nullable = false)
    val updatedAt: LocalDateTime = LocalDateTime.now()
) {
    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false
        other as DataEntity
        return externalId == other.externalId
    }
    
    override fun hashCode(): Int {
        return externalId.hashCode()
    }
}