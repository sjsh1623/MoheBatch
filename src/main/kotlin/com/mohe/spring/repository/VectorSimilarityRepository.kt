package com.mohe.spring.repository

import com.mohe.spring.entity.VectorSimilarity
import com.mohe.spring.entity.VectorSimilarityId
import org.springframework.data.domain.Pageable
import org.springframework.data.jpa.repository.JpaRepository
import org.springframework.data.jpa.repository.Modifying
import org.springframework.data.jpa.repository.Query
import org.springframework.stereotype.Repository
import java.time.OffsetDateTime

@Repository
interface VectorSimilarityRepository : JpaRepository<VectorSimilarity, VectorSimilarityId> {
    fun findByUserIdAndPlaceId(userId: Long, placeId: Long): VectorSimilarity?
    fun findByUserIdOrderByWeightedSimilarityDesc(userId: Long, pageable: Pageable): List<VectorSimilarity>
    fun deleteByUserId(userId: Long)
    fun deleteByPlaceId(placeId: Long)
    
    @Modifying
    @Query("DELETE FROM VectorSimilarity vs WHERE vs.calculatedAt < :cutoffDate")
    fun deleteOlderThan(cutoffDate: OffsetDateTime): Int
    
    @Query(
        "SELECT vs FROM VectorSimilarity vs WHERE vs.userId = :userId " +
        "AND vs.weightedSimilarity >= :minThreshold ORDER BY vs.weightedSimilarity DESC"
    )
    fun findTopSimilarPlaces(userId: Long, minThreshold: Double, pageable: Pageable): List<VectorSimilarity>
}