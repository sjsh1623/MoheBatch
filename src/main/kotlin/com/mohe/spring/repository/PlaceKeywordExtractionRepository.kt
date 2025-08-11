package com.mohe.spring.repository

import com.mohe.spring.entity.PlaceKeywordExtraction
import org.springframework.data.jpa.repository.JpaRepository
import org.springframework.data.jpa.repository.Modifying
import org.springframework.data.jpa.repository.Query
import org.springframework.data.repository.query.Param
import org.springframework.stereotype.Repository
import org.springframework.transaction.annotation.Transactional
import java.time.LocalDateTime

@Repository
interface PlaceKeywordExtractionRepository : JpaRepository<PlaceKeywordExtraction, Long> {
    
    /**
     * Find keyword extraction by place ID
     */
    fun findByPlaceId(placeId: Long): PlaceKeywordExtraction?
    
    /**
     * Find all extractions for multiple places
     */
    fun findByPlaceIdIn(placeIds: List<Long>): List<PlaceKeywordExtraction>
    
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
    fun findSimilarPlacesByVector(@Param("placeId") placeId: Long, @Param("limit") limit: Int): List<Long>
    
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
    fun findSimilarPlacesWithScores(@Param("placeId") placeId: Long, @Param("limit") limit: Int): List<Array<Any>>
    
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
    fun findSimilarPlacesByThreshold(
        @Param("placeId") placeId: Long, 
        @Param("threshold") threshold: Double
    ): List<Long>
    
    /**
     * Delete extraction for a place (for reprocessing)
     */
    @Modifying
    @Transactional
    fun deleteByPlaceId(placeId: Long): Int
    
    /**
     * Find extractions by model name and version
     */
    fun findByModelNameAndModelVersion(modelName: String, modelVersion: String): List<PlaceKeywordExtraction>
    
    /**
     * Find extractions older than a specific date (for cleanup)
     */
    fun findByCreatedAtBefore(cutoffDate: LocalDateTime): List<PlaceKeywordExtraction>
    
    /**
     * Count total extractions
     */
    fun countByModelName(modelName: String): Long
    
    /**
     * Find extractions with processing time above threshold (for performance monitoring)
     */
    fun findByProcessingTimeMsGreaterThan(thresholdMs: Int): List<PlaceKeywordExtraction>
    
    /**
     * Get average processing time by model
     */
    @Query("SELECT AVG(pke.processingTimeMs) FROM PlaceKeywordExtraction pke WHERE pke.modelName = :modelName")
    fun getAverageProcessingTimeByModel(@Param("modelName") modelName: String): Double?
    
    /**
     * Batch update extraction method for all records
     */
    @Modifying
    @Transactional
    @Query("UPDATE PlaceKeywordExtraction pke SET pke.extractionMethod = :newMethod WHERE pke.extractionMethod = :oldMethod")
    fun updateExtractionMethod(@Param("oldMethod") oldMethod: String, @Param("newMethod") newMethod: String): Int
    
    /**
     * Check if a place has keyword extraction
     */
    fun existsByPlaceId(placeId: Long): Boolean
    
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
    fun findPlacesNeedingExtraction(@Param("limit") limit: Int): List<Long>
    
    /**
     * Search by keyword content in selected_keywords JSONB
     */
    @Query(value = """
        SELECT * FROM place_keyword_extractions pke
        WHERE pke.selected_keywords::jsonb @> :keywordJson::jsonb
    """, nativeQuery = true)
    fun findBySelectedKeywordsContaining(@Param("keywordJson") keywordJson: String): List<PlaceKeywordExtraction>
}