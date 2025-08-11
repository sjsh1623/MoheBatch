package com.mohe.spring.entity

import com.fasterxml.jackson.databind.JsonNode
import com.vladmihalcea.hibernate.type.json.JsonType
import jakarta.persistence.*
import org.hibernate.annotations.Type
import java.time.OffsetDateTime

@Entity
@Table(name = "user_preference_vectors")
data class UserPreferenceVector(
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    val id: Long = 0,
    
    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "user_id", nullable = false)
    val user: User,
    
    @Column(name = "raw_profile_text", nullable = false, columnDefinition = "TEXT")
    val rawProfileText: String,
    
    @Column(name = "combined_preferences_text", columnDefinition = "TEXT")
    val combinedPreferencesText: String? = null,
    
    // 100-dimensional vector stored as PostgreSQL vector type
    @Column(name = "preference_vector", nullable = false, columnDefinition = "vector(100)")
    val preferenceVector: String, // Stored as string representation: "[0.1, 0.0, 0.8, ...]"
    
    // Selected 15 keywords with confidences
    @Type(JsonType::class)
    @Column(name = "selected_keywords", nullable = false, columnDefinition = "jsonb")
    val selectedKeywords: JsonNode, // [{"keyword_id": 1, "keyword": "specialty_coffee", "confidence": 0.85}, ...]
    
    @Column(name = "extraction_source", nullable = false)
    val extractionSource: String = "ollama-openai",
    
    @Column(name = "model_name", nullable = false)
    val modelName: String,
    
    @Column(name = "model_version")
    val modelVersion: String? = null,
    
    @Column(name = "extraction_prompt_hash")
    val extractionPromptHash: String? = null,
    
    @Column(name = "created_at")
    val createdAt: OffsetDateTime = OffsetDateTime.now(),
    
    @Column(name = "updated_at")
    val updatedAt: OffsetDateTime = OffsetDateTime.now()
) {
    /**
     * Parse the vector string into a float array
     */
    fun getVectorAsFloatArray(): FloatArray {
        val vectorStr = preferenceVector.trim('[', ']')
        return vectorStr.split(',')
            .map { it.trim().toFloat() }
            .toFloatArray()
    }
    
    /**
     * Get selected keywords as a typed list
     */
    fun getSelectedKeywordsList(): List<SelectedKeyword> {
        val keywords = mutableListOf<SelectedKeyword>()
        selectedKeywords.forEach { node ->
            keywords.add(
                SelectedKeyword(
                    keywordId = node["keyword_id"].asInt(),
                    keyword = node["keyword"].asText(),
                    confidence = node["confidence"].asDouble()
                )
            )
        }
        return keywords
    }
    
    /**
     * Get keyword IDs that have confidence above threshold
     */
    fun getHighConfidenceKeywordIds(threshold: Double = 0.5): List<Int> {
        return getSelectedKeywordsList()
            .filter { it.confidence >= threshold }
            .map { it.keywordId }
    }
    
    /**
     * Calculate Jaccard similarity with another vector based on keyword overlap
     */
    fun calculateJaccardSimilarity(other: UserPreferenceVector, confidenceThreshold: Double = 0.3): Double {
        val thisKeywords = getSelectedKeywordsList()
            .filter { it.confidence >= confidenceThreshold }
            .map { it.keywordId }
            .toSet()
            
        val otherKeywords = other.getSelectedKeywordsList()
            .filter { it.confidence >= confidenceThreshold }
            .map { it.keywordId }
            .toSet()
            
        if (thisKeywords.isEmpty() && otherKeywords.isEmpty()) return 1.0
        
        val intersection = thisKeywords.intersect(otherKeywords).size
        val union = thisKeywords.union(otherKeywords).size
        
        return intersection.toDouble() / union.toDouble()
    }
    
    /**
     * Calculate weighted cosine similarity considering confidence scores
     */
    fun calculateWeightedCosineSimilarity(other: UserPreferenceVector): Double {
        val thisVector = getVectorAsFloatArray()
        val otherVector = other.getVectorAsFloatArray()
        
        if (thisVector.size != otherVector.size) {
            throw IllegalArgumentException("Vector dimensions must match")
        }
        
        var dotProduct = 0.0
        var normThis = 0.0
        var normOther = 0.0
        
        for (i in thisVector.indices) {
            dotProduct += thisVector[i] * otherVector[i]
            normThis += thisVector[i] * thisVector[i]
            normOther += otherVector[i] * otherVector[i]
        }
        
        val normProduct = kotlin.math.sqrt(normThis * normOther)
        return if (normProduct > 0) dotProduct / normProduct else 0.0
    }
}

/**
 * Data class representing a selected keyword with confidence
 */
data class SelectedKeyword(
    val keywordId: Int,
    val keyword: String,
    val confidence: Double
)