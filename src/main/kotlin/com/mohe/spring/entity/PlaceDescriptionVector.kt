package com.mohe.spring.entity

import com.fasterxml.jackson.databind.JsonNode
import com.vladmihalcea.hibernate.type.json.JsonType
import jakarta.persistence.*
import org.hibernate.annotations.Type
import java.time.OffsetDateTime

@Entity
@Table(name = "place_description_vectors")
data class PlaceDescriptionVector(
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    val id: Long = 0,
    
    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "place_id", nullable = false)
    val place: Place,
    
    @Column(name = "raw_description_text", nullable = false, columnDefinition = "TEXT")
    val rawDescriptionText: String,
    
    @Column(name = "combined_attributes_text", columnDefinition = "TEXT")
    val combinedAttributesText: String? = null,
    
    // 100-dimensional vector stored as PostgreSQL vector type
    @Column(name = "description_vector", nullable = false, columnDefinition = "vector(100)")
    val descriptionVector: String, // Stored as string representation: "[0.1, 0.0, 0.8, ...]"
    
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
        val vectorStr = descriptionVector.trim('[', ']')
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
     * Calculate similarity with user preference vector
     */
    fun calculateSimilarityWithUser(userVector: UserPreferenceVector, mbtiBoost: Double = 1.2): VectorSimilarityResult {
        val cosineScore = calculateWeightedCosineSimilarity(userVector)
        val jaccardScore = calculateJaccardSimilarity(userVector)
        val euclideanDistance = calculateEuclideanDistance(userVector)
        
        // Apply MBTI-based boost if applicable
        val mbtiBoostFactor = calculateMbtiBoost(userVector, mbtiBoost)
        val weightedScore = (cosineScore * 0.7 + jaccardScore * 0.3) * mbtiBoostFactor
        
        val commonKeywords = getCommonKeywordIds(userVector)
        
        return VectorSimilarityResult(
            cosineSimilarity = cosineScore,
            jaccardSimilarity = jaccardScore,
            euclideanDistance = euclideanDistance,
            mbtiBoostFactor = mbtiBoostFactor,
            weightedSimilarity = weightedScore.coerceAtMost(1.0),
            commonKeywords = commonKeywords.size,
            keywordOverlapRatio = if (getSelectedKeywordsList().size > 0) 
                commonKeywords.size.toDouble() / 15.0 else 0.0
        )
    }
    
    /**
     * Calculate Jaccard similarity based on keyword overlap
     */
    private fun calculateJaccardSimilarity(userVector: UserPreferenceVector, confidenceThreshold: Double = 0.3): Double {
        val thisKeywords = getSelectedKeywordsList()
            .filter { it.confidence >= confidenceThreshold }
            .map { it.keywordId }
            .toSet()
            
        val userKeywords = userVector.getSelectedKeywordsList()
            .filter { it.confidence >= confidenceThreshold }
            .map { it.keywordId }
            .toSet()
            
        if (thisKeywords.isEmpty() && userKeywords.isEmpty()) return 1.0
        
        val intersection = thisKeywords.intersect(userKeywords).size
        val union = thisKeywords.union(userKeywords).size
        
        return intersection.toDouble() / union.toDouble()
    }
    
    /**
     * Calculate weighted cosine similarity
     */
    private fun calculateWeightedCosineSimilarity(userVector: UserPreferenceVector): Double {
        val thisVector = getVectorAsFloatArray()
        val userVectorArray = userVector.getVectorAsFloatArray()
        
        if (thisVector.size != userVectorArray.size) {
            throw IllegalArgumentException("Vector dimensions must match")
        }
        
        var dotProduct = 0.0
        var normThis = 0.0
        var normUser = 0.0
        
        for (i in thisVector.indices) {
            dotProduct += thisVector[i] * userVectorArray[i]
            normThis += thisVector[i] * thisVector[i]
            normUser += userVectorArray[i] * userVectorArray[i]
        }
        
        val normProduct = kotlin.math.sqrt(normThis * normUser)
        return if (normProduct > 0) dotProduct / normProduct else 0.0
    }
    
    /**
     * Calculate Euclidean distance
     */
    private fun calculateEuclideanDistance(userVector: UserPreferenceVector): Double {
        val thisVector = getVectorAsFloatArray()
        val userVectorArray = userVector.getVectorAsFloatArray()
        
        var sumOfSquares = 0.0
        for (i in thisVector.indices) {
            val diff = thisVector[i] - userVectorArray[i]
            sumOfSquares += diff * diff
        }
        
        return kotlin.math.sqrt(sumOfSquares)
    }
    
    /**
     * Calculate MBTI-based similarity boost
     */
    private fun calculateMbtiBoost(userVector: UserPreferenceVector, baseBoost: Double): Double {
        // Simple implementation - can be enhanced with the keyword catalog rules
        val userMbti = userVector.user.mbti ?: return 1.0
        val placeKeywords = getHighConfidenceKeywordIds(0.4)
        
        // Example MBTI-based boosts based on keyword patterns
        return when {
            // Introverts prefer quiet, study-friendly places
            userMbti.startsWith("I") && placeKeywords.any { it in listOf(16, 38, 24, 98) } -> baseBoost
            // Extraverts prefer social, buzzing places  
            userMbti.startsWith("E") && placeKeywords.any { it in listOf(26, 43, 96, 99) } -> baseBoost
            // Intuitives prefer creative, unique places
            userMbti.contains("N") && placeKeywords.any { it in listOf(23, 85, 91, 93) } -> baseBoost
            // Sensors prefer traditional, reliable places
            userMbti.contains("S") && placeKeywords.any { it in listOf(87, 90, 51, 68) } -> baseBoost
            else -> 1.0
        }
    }
    
    /**
     * Get common keyword IDs with user vector
     */
    private fun getCommonKeywordIds(userVector: UserPreferenceVector, confidenceThreshold: Double = 0.3): List<Int> {
        val thisKeywords = getSelectedKeywordsList()
            .filter { it.confidence >= confidenceThreshold }
            .map { it.keywordId }
            .toSet()
            
        val userKeywords = userVector.getSelectedKeywordsList()
            .filter { it.confidence >= confidenceThreshold }
            .map { it.keywordId }
            .toSet()
            
        return thisKeywords.intersect(userKeywords).toList()
    }
}

/**
 * Result of vector similarity calculation
 */
data class VectorSimilarityResult(
    val cosineSimilarity: Double,
    val jaccardSimilarity: Double,
    val euclideanDistance: Double,
    val mbtiBoostFactor: Double,
    val weightedSimilarity: Double,
    val commonKeywords: Int,
    val keywordOverlapRatio: Double
)