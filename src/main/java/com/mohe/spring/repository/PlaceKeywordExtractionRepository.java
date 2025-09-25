package com.mohe.spring.repository;

import com.mohe.spring.entity.PlaceKeywordExtraction;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;

@Repository
public interface PlaceKeywordExtractionRepository extends JpaRepository<PlaceKeywordExtraction, Long> {
    
    /**
     * Find keyword extraction by place ID
     */
    Optional<PlaceKeywordExtraction> findByPlaceId(Long placeId);
    
    /**
     * Find all extractions for multiple places
     */
    List<PlaceKeywordExtraction> findByPlaceIdIn(List<Long> placeIds);
    
    /**
     * Find similar places using pgvector cosine similarity
     * Returns place IDs ordered by similarity score (most similar first)
     */
    @Query(value = """
        SELECT pke.place_id 
        FROM place_keyword_extractions pke 
        WHERE pke.place_id != :placeId 
        ORDER BY pke.keyword_vector <=> (
            SELECT keyword_vector 
            FROM place_keyword_extractions 
            WHERE place_id = :placeId
        ) 
        LIMIT :limit
    """, nativeQuery = true)
    List<Long> findSimilarPlacesByVector(@Param("placeId") Long placeId, @Param("limit") int limit);
    
    /**
     * Find similar places with similarity scores
     * Returns both place ID and similarity score
     */
    @Query(value = """
        SELECT pke.place_id, 
               1 - (pke.keyword_vector <=> target.keyword_vector) as similarity_score
        FROM place_keyword_extractions pke,
             (SELECT keyword_vector FROM place_keyword_extractions WHERE place_id = :placeId) target
        WHERE pke.place_id != :placeId 
        ORDER BY similarity_score DESC 
        LIMIT :limit
    """, nativeQuery = true)
    List<Object[]> findSimilarPlacesWithScores(@Param("placeId") Long placeId, @Param("limit") int limit);
    
    /**
     * Find places by keyword vector similarity threshold
     */
    @Query(value = """
        SELECT pke.place_id 
        FROM place_keyword_extractions pke 
        WHERE pke.place_id != :placeId 
        AND (1 - (pke.keyword_vector <=> (
            SELECT keyword_vector 
            FROM place_keyword_extractions 
            WHERE place_id = :placeId
        ))) >= :threshold
        ORDER BY pke.keyword_vector <=> (
            SELECT keyword_vector 
            FROM place_keyword_extractions 
            WHERE place_id = :placeId
        )
    """, nativeQuery = true)
    List<Long> findSimilarPlacesByThreshold(
        @Param("placeId") Long placeId, 
        @Param("threshold") Double threshold
    );
    
    /**
     * Delete extraction for a place (for reprocessing)
     */
    @Modifying
    @Transactional
    int deleteByPlaceId(Long placeId);
    
    /**
     * Find extractions by model name and version
     */
    List<PlaceKeywordExtraction> findByModelNameAndModelVersion(String modelName, String modelVersion);
    
    /**
     * Find extractions older than a specific date (for cleanup)
     */
    List<PlaceKeywordExtraction> findByCreatedAtBefore(LocalDateTime cutoffDate);
    
    /**
     * Count total extractions
     */
    long countByModelName(String modelName);
    
    /**
     * Find extractions with processing time above threshold (for performance monitoring)
     */
    List<PlaceKeywordExtraction> findByProcessingTimeMsGreaterThan(int thresholdMs);
    
    /**
     * Get average processing time by model
     */
    @Query("SELECT AVG(pke.processingTimeMs) FROM PlaceKeywordExtraction pke WHERE pke.modelName = :modelName")
    Optional<Double> getAverageProcessingTimeByModel(@Param("modelName") String modelName);
    
    /**
     * Batch update extraction method for all records
     */
    @Modifying
    @Transactional
    @Query("UPDATE PlaceKeywordExtraction pke SET pke.extractionMethod = :newMethod WHERE pke.extractionMethod = :oldMethod")
    int updateExtractionMethod(@Param("oldMethod") String oldMethod, @Param("newMethod") String newMethod);
    
    /**
     * Check if a place has keyword extraction
     */
    boolean existsByPlaceId(Long placeId);
    
    /**
     * Find places that need reprocessing (missing extractions)
     */
    @Query(value = """
        SELECT p.id 
        FROM places p 
        LEFT JOIN place_keyword_extractions pke ON p.id = pke.place_id 
        WHERE pke.id IS NULL
        LIMIT :limit
    """, nativeQuery = true)
    List<Long> findPlacesNeedingExtraction(@Param("limit") int limit);
    
    /**
     * Search by keyword content in selected_keywords JSONB
     */
    @Query(value = """
        SELECT * FROM place_keyword_extractions pke
        WHERE pke.selected_keywords::jsonb @> :keywordJson::jsonb
    """, nativeQuery = true)
    List<PlaceKeywordExtraction> findBySelectedKeywordsContaining(@Param("keywordJson") String keywordJson);
}