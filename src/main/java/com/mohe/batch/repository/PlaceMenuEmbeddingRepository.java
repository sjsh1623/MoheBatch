package com.mohe.batch.repository;

import com.mohe.batch.entity.PlaceMenuEmbedding;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

@Repository
public interface PlaceMenuEmbeddingRepository extends JpaRepository<PlaceMenuEmbedding, Long> {

    /**
     * Find all menu embeddings for a specific place
     */
    List<PlaceMenuEmbedding> findByPlaceId(Long placeId);

    /**
     * Delete all menu embeddings for a specific place
     */
    @Modifying
    @Transactional
    @Query("DELETE FROM PlaceMenuEmbedding e WHERE e.placeId = :placeId")
    void deleteByPlaceId(@Param("placeId") Long placeId);

    /**
     * Delete menu embedding by menu ID
     */
    @Modifying
    @Transactional
    @Query("DELETE FROM PlaceMenuEmbedding e WHERE e.menuId = :menuId")
    void deleteByMenuId(@Param("menuId") Long menuId);

    /**
     * Count menu embeddings for a specific place
     */
    long countByPlaceId(Long placeId);

    /**
     * Find distinct place IDs that have menu embeddings
     */
    @Query("SELECT DISTINCT e.placeId FROM PlaceMenuEmbedding e")
    List<Long> findDistinctPlaceIds();

    /**
     * Check if a place has any menu embeddings
     */
    boolean existsByPlaceId(Long placeId);

    /**
     * Check if a menu has embedding
     */
    boolean existsByMenuId(Long menuId);
}
