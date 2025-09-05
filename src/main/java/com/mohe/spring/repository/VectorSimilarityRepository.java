package com.mohe.spring.repository;

import com.mohe.spring.entity.VectorSimilarity;
import com.mohe.spring.entity.VectorSimilarityId;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.Optional;

@Repository
public interface VectorSimilarityRepository extends JpaRepository<VectorSimilarity, VectorSimilarityId> {
    
    Optional<VectorSimilarity> findByUserIdAndPlaceId(Long userId, Long placeId);
    
    List<VectorSimilarity> findByUserIdOrderByWeightedSimilarityDesc(Long userId, Pageable pageable);
    
    void deleteByUserId(Long userId);
    
    void deleteByPlaceId(Long placeId);
    
    @Modifying
    @Query("DELETE FROM VectorSimilarity vs WHERE vs.calculatedAt < :cutoffDate")
    int deleteOlderThan(OffsetDateTime cutoffDate);
    
    @Query(
        "SELECT vs FROM VectorSimilarity vs WHERE vs.userId = :userId " +
        "AND vs.weightedSimilarity >= :minThreshold ORDER BY vs.weightedSimilarity DESC"
    )
    List<VectorSimilarity> findTopSimilarPlaces(Long userId, Double minThreshold, Pageable pageable);
}