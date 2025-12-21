package com.mohe.batch.repository;

import com.mohe.batch.entity.PlaceKeywordEmbedding;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

@Repository
public interface PlaceKeywordEmbeddingRepository extends JpaRepository<PlaceKeywordEmbedding, Long> {

    /**
     * Find all embeddings for a specific place
     */
    List<PlaceKeywordEmbedding> findByPlaceId(Long placeId);

    /**
     * Delete all embeddings for a specific place
     */
    @Modifying
    @Transactional
    @Query("DELETE FROM PlaceKeywordEmbedding e WHERE e.placeId = :placeId")
    void deleteByPlaceId(@Param("placeId") Long placeId);

    /**
     * Count embeddings for a specific place
     */
    long countByPlaceId(Long placeId);

    /**
     * Find distinct place IDs that have embeddings
     */
    @Query("SELECT DISTINCT e.placeId FROM PlaceKeywordEmbedding e")
    List<Long> findDistinctPlaceIds();

    /**
     * Check if a place has any embeddings
     */
    boolean existsByPlaceId(Long placeId);
}
