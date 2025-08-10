package com.mohe.spring.repository

import com.mohe.spring.entity.PlaceMbtiDescription
import org.springframework.data.jpa.repository.JpaRepository
import org.springframework.data.jpa.repository.Modifying
import org.springframework.data.jpa.repository.Query
import org.springframework.data.repository.query.Param
import org.springframework.stereotype.Repository
import java.time.LocalDateTime

@Repository
interface PlaceMbtiDescriptionRepository : JpaRepository<PlaceMbtiDescription, Long> {
    
    fun findByPlaceIdAndMbti(placeId: Long, mbti: String): PlaceMbtiDescription?
    
    fun findByPlaceId(placeId: Long): List<PlaceMbtiDescription>
    
    fun findByMbti(mbti: String): List<PlaceMbtiDescription>
    
    @Query("SELECT p FROM PlaceMbtiDescription p WHERE p.placeId IN :placeIds")
    fun findByPlaceIdIn(@Param("placeIds") placeIds: List<Long>): List<PlaceMbtiDescription>
    
    @Query("SELECT p FROM PlaceMbtiDescription p WHERE p.placeId IN :placeIds AND p.mbti = :mbti")
    fun findByPlaceIdInAndMbti(@Param("placeIds") placeIds: List<Long>, @Param("mbti") mbti: String): List<PlaceMbtiDescription>
    
    @Modifying
    @Query("""
        INSERT INTO place_mbti_descriptions (place_id, mbti, description, model, prompt_hash, updated_at)
        VALUES (:placeId, :mbti, :description, :model, :promptHash, :updatedAt)
        ON CONFLICT (place_id, mbti) 
        DO UPDATE SET 
            description = EXCLUDED.description,
            model = EXCLUDED.model,
            prompt_hash = EXCLUDED.prompt_hash,
            updated_at = EXCLUDED.updated_at
        WHERE place_mbti_descriptions.prompt_hash != EXCLUDED.prompt_hash
    """, nativeQuery = true)
    fun upsertMbtiDescription(
        @Param("placeId") placeId: Long,
        @Param("mbti") mbti: String,
        @Param("description") description: String,
        @Param("model") model: String,
        @Param("promptHash") promptHash: String,
        @Param("updatedAt") updatedAt: LocalDateTime
    )
    
    @Query("""
        SELECT COUNT(*) FROM place_mbti_descriptions 
        WHERE place_id = :placeId AND updated_at > :since
    """, nativeQuery = true)
    fun countRecentDescriptions(@Param("placeId") placeId: Long, @Param("since") since: LocalDateTime): Long
    
    @Modifying
    @Query("DELETE FROM PlaceMbtiDescription p WHERE p.updatedAt < :cutoffDate")
    fun deleteOldDescriptions(@Param("cutoffDate") cutoffDate: LocalDateTime)
}