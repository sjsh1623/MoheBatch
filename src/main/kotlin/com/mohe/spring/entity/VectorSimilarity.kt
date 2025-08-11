package com.mohe.spring.entity

import jakarta.persistence.*
import java.math.BigDecimal
import java.time.OffsetDateTime

@Entity
@Table(name = "vector_similarities")
@IdClass(VectorSimilarityId::class)
data class VectorSimilarity(
    @Id
    @Column(name = "user_id")
    val userId: Long,
    
    @Id  
    @Column(name = "place_id")
    val placeId: Long,
    
    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "user_id", insertable = false, updatable = false)
    val user: User? = null,
    
    @ManyToOne(fetch = FetchType.LAZY) 
    @JoinColumn(name = "place_id", insertable = false, updatable = false)
    val place: Place? = null,
    
    @Column(name = "cosine_similarity", precision = 5, scale = 4, nullable = false)
    val cosineSimilarity: BigDecimal,
    
    @Column(name = "euclidean_distance", precision = 8, scale = 4, nullable = false)
    val euclideanDistance: BigDecimal,
    
    @Column(name = "jaccard_similarity", precision = 5, scale = 4)
    val jaccardSimilarity: BigDecimal? = null,
    
    @Column(name = "mbti_boost_factor", precision = 3, scale = 2)
    val mbtiBoostFactor: BigDecimal = BigDecimal("1.0"),
    
    @Column(name = "weighted_similarity", precision = 5, scale = 4, nullable = false)
    val weightedSimilarity: BigDecimal,
    
    @Column(name = "common_keywords", nullable = false)
    val commonKeywords: Int = 0,
    
    @Column(name = "keyword_overlap_ratio", precision = 3, scale = 2)
    val keywordOverlapRatio: BigDecimal? = null,
    
    @Column(name = "calculated_at")
    val calculatedAt: OffsetDateTime = OffsetDateTime.now(),
    
    @Column(name = "user_vector_version")
    val userVectorVersion: Long? = null,
    
    @Column(name = "place_vector_version")  
    val placeVectorVersion: Long? = null
) {
    /**
     * Check if this similarity calculation is recent and valid
     */
    fun isRecent(hoursThreshold: Long = 24): Boolean {
        return calculatedAt.isAfter(OffsetDateTime.now().minusHours(hoursThreshold))
    }
    
    /**
     * Get similarity score as double
     */
    fun getWeightedSimilarityAsDouble(): Double {
        return weightedSimilarity.toDouble()
    }
    
    /**
     * Get cosine similarity as double
     */
    fun getCosineSimilarityAsDouble(): Double {
        return cosineSimilarity.toDouble()
    }
    
    /**
     * Get Jaccard similarity as double
     */
    fun getJaccardSimilarityAsDouble(): Double {
        return jaccardSimilarity?.toDouble() ?: 0.0
    }
    
    /**
     * Check if vectors are compatible (high keyword overlap)
     */
    fun hasHighKeywordOverlap(threshold: Double = 0.3): Boolean {
        return keywordOverlapRatio?.toDouble()?.let { it >= threshold } ?: false
    }
    
    /**
     * Get MBTI boost factor as double
     */
    fun getMbtiBoostFactorAsDouble(): Double {
        return mbtiBoostFactor.toDouble()
    }
    
    companion object {
        /**
         * Create VectorSimilarity from calculation result
         */
        fun fromCalculationResult(
            userId: Long,
            placeId: Long, 
            result: VectorSimilarityResult,
            userVectorVersion: Long? = null,
            placeVectorVersion: Long? = null
        ): VectorSimilarity {
            return VectorSimilarity(
                userId = userId,
                placeId = placeId,
                cosineSimilarity = BigDecimal(result.cosineSimilarity).setScale(4, java.math.RoundingMode.HALF_UP),
                euclideanDistance = BigDecimal(result.euclideanDistance).setScale(4, java.math.RoundingMode.HALF_UP),
                jaccardSimilarity = BigDecimal(result.jaccardSimilarity).setScale(4, java.math.RoundingMode.HALF_UP),
                mbtiBoostFactor = BigDecimal(result.mbtiBoostFactor).setScale(2, java.math.RoundingMode.HALF_UP),
                weightedSimilarity = BigDecimal(result.weightedSimilarity).setScale(4, java.math.RoundingMode.HALF_UP),
                commonKeywords = result.commonKeywords,
                keywordOverlapRatio = BigDecimal(result.keywordOverlapRatio).setScale(2, java.math.RoundingMode.HALF_UP),
                userVectorVersion = userVectorVersion,
                placeVectorVersion = placeVectorVersion
            )
        }
    }
}

/**
 * Composite primary key for VectorSimilarity
 */
data class VectorSimilarityId(
    val userId: Long = 0,
    val placeId: Long = 0
) : java.io.Serializable