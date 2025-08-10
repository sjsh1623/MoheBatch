package com.mohe.spring.entity

import jakarta.persistence.*
import java.math.BigDecimal
import java.time.LocalDateTime

@Entity
@Table(
    name = "place_similarity",
    indexes = [
        Index(name = "idx_place_similarity_place1", columnList = "place_id1"),
        Index(name = "idx_place_similarity_place2", columnList = "place_id2"),
        Index(name = "idx_place_similarity_jaccard", columnList = "jaccard"),
        Index(name = "idx_place_similarity_cosine", columnList = "cosine_bin")
    ]
)
@IdClass(PlaceSimilarityId::class)
data class PlaceSimilarity(
    @Id
    @Column(name = "place_id1", nullable = false)
    val placeId1: Long,
    
    @Id  
    @Column(name = "place_id2", nullable = false)
    val placeId2: Long,
    
    @Column(precision = 5, scale = 4)
    val jaccard: BigDecimal = BigDecimal.ZERO,
    
    @Column(name = "cosine_bin", precision = 5, scale = 4)
    val cosineBin: BigDecimal = BigDecimal.ZERO,
    
    @Column(name = "co_users")
    val coUsers: Int = 0,
    
    @Column(name = "updated_at")
    val updatedAt: LocalDateTime = LocalDateTime.now(),
    
    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "place_id1", insertable = false, updatable = false)
    val place1: Place? = null,
    
    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "place_id2", insertable = false, updatable = false)  
    val place2: Place? = null
)

@Embeddable
data class PlaceSimilarityId(
    val placeId1: Long = 0,
    val placeId2: Long = 0
) : java.io.Serializable

@Entity
@Table(
    name = "place_similarity_topk",
    indexes = [
        Index(name = "idx_place_similarity_topk_place", columnList = "place_id, rank"),
        Index(name = "idx_place_similarity_topk_neighbor", columnList = "neighbor_place_id")
    ],
    uniqueConstraints = [UniqueConstraint(columnNames = ["place_id", "rank"])]
)
@IdClass(PlaceSimilarityTopKId::class)
data class PlaceSimilarityTopK(
    @Id
    @Column(name = "place_id", nullable = false)
    val placeId: Long,
    
    @Id
    @Column(name = "neighbor_place_id", nullable = false)
    val neighborPlaceId: Long,
    
    @Column(nullable = false)
    val rank: Short,
    
    @Column(precision = 5, scale = 4)
    val jaccard: BigDecimal = BigDecimal.ZERO,
    
    @Column(name = "cosine_bin", precision = 5, scale = 4)
    val cosineBin: BigDecimal = BigDecimal.ZERO,
    
    @Column(name = "co_users")
    val coUsers: Int = 0,
    
    @Column(name = "updated_at")
    val updatedAt: LocalDateTime = LocalDateTime.now(),
    
    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "place_id", insertable = false, updatable = false)
    val place: Place? = null,
    
    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "neighbor_place_id", insertable = false, updatable = false)
    val neighborPlace: Place? = null
)

@Embeddable
data class PlaceSimilarityTopKId(
    val placeId: Long = 0,
    val neighborPlaceId: Long = 0
) : java.io.Serializable

@Entity
@Table(
    name = "place_similarity_mbti_topk",
    indexes = [
        Index(name = "idx_place_similarity_mbti_topk_place_mbti", columnList = "place_id, mbti, rank")
    ],
    uniqueConstraints = [UniqueConstraint(columnNames = ["place_id", "mbti", "rank"])]
)
@IdClass(PlaceSimilarityMbtiTopKId::class)
data class PlaceSimilarityMbtiTopK(
    @Id
    @Column(name = "place_id", nullable = false)
    val placeId: Long,
    
    @Id
    @Column(nullable = false, length = 4)
    val mbti: String,
    
    @Id
    @Column(name = "neighbor_place_id", nullable = false)
    val neighborPlaceId: Long,
    
    @Column(nullable = false)
    val rank: Short,
    
    @Column(precision = 5, scale = 4)
    val jaccard: BigDecimal = BigDecimal.ZERO,
    
    @Column(name = "cosine_bin", precision = 5, scale = 4)
    val cosineBin: BigDecimal = BigDecimal.ZERO,
    
    @Column(name = "co_users")
    val coUsers: Int = 0,
    
    @Column(name = "updated_at")
    val updatedAt: LocalDateTime = LocalDateTime.now(),
    
    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "place_id", insertable = false, updatable = false)
    val place: Place? = null,
    
    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "neighbor_place_id", insertable = false, updatable = false)
    val neighborPlace: Place? = null
)

@Embeddable
data class PlaceSimilarityMbtiTopKId(
    val placeId: Long = 0,
    val mbti: String = "",
    val neighborPlaceId: Long = 0
) : java.io.Serializable