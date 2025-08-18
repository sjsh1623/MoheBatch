package com.mohe.spring.entity

import jakarta.persistence.*
import java.time.LocalDateTime

@Entity
@Table(name = "keyword_catalog")
data class KeywordCatalog(
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    val id: Long = 0,
    
    @Column(name = "keyword", nullable = false, unique = true, length = 100)
    val keyword: String,
    
    @Column(name = "definition", nullable = false, columnDefinition = "TEXT")
    val definition: String,
    
    @Column(name = "category", nullable = false, length = 50)
    val category: String,
    
    @Column(name = "related_groups", columnDefinition = "TEXT")
    val relatedGroups: String = "", // Store as comma-separated string
    
    @Column(name = "vector_position", nullable = false)
    val vectorPosition: Int, // Position in the 100-dimensional vector (0-99)
    
    @Column(name = "created_at")
    val createdAt: LocalDateTime = LocalDateTime.now(),
    
    @Column(name = "updated_at")
    val updatedAt: LocalDateTime = LocalDateTime.now()
) {
    /**
     * Get related groups as a list
     */
    fun getRelatedGroupsList(): List<String> {
        return if (relatedGroups.isBlank()) emptyList() 
               else relatedGroups.split(",").map { it.trim() }
    }
}