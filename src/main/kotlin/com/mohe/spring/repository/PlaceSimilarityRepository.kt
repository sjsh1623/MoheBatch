package com.mohe.spring.repository

import com.mohe.spring.entity.PlaceSimilarity
import com.mohe.spring.entity.PlaceSimilarityId
import com.mohe.spring.entity.PlaceSimilarityTopK
import com.mohe.spring.entity.PlaceSimilarityTopKId
import org.springframework.data.jpa.repository.JpaRepository
import org.springframework.data.jpa.repository.Modifying
import org.springframework.data.jpa.repository.Query
import org.springframework.data.repository.query.Param
import org.springframework.stereotype.Repository
import java.math.BigDecimal
import java.time.LocalDateTime

@Repository
interface PlaceSimilarityRepository : JpaRepository<PlaceSimilarity, PlaceSimilarityId> {
    
    @Query("""
        SELECT s FROM PlaceSimilarity s 
        WHERE s.placeId1 = :placeId OR s.placeId2 = :placeId
        ORDER BY (s.jaccard + s.cosineBin) DESC
    """)
    fun findSimilaritiesByPlaceId(@Param("placeId") placeId: Long): List<PlaceSimilarity>
    
    @Modifying
    @Query("""
        INSERT INTO place_similarity (place_id1, place_id2, jaccard, cosine_bin, co_users, updated_at)
        VALUES (LEAST(:placeId1, :placeId2), GREATEST(:placeId1, :placeId2), :jaccard, :cosineBin, :coUsers, :updatedAt)
        ON CONFLICT (place_id1, place_id2) 
        DO UPDATE SET 
            jaccard = EXCLUDED.jaccard,
            cosine_bin = EXCLUDED.cosine_bin,
            co_users = EXCLUDED.co_users,
            updated_at = EXCLUDED.updated_at
    """, nativeQuery = true)
    fun upsertSimilarity(
        @Param("placeId1") placeId1: Long,
        @Param("placeId2") placeId2: Long,
        @Param("jaccard") jaccard: BigDecimal,
        @Param("cosineBin") cosineBin: BigDecimal,
        @Param("coUsers") coUsers: Int,
        @Param("updatedAt") updatedAt: LocalDateTime
    )
    
    @Query("""
        SELECT p1.id AS place_id, p2.id AS other_place_id, 
               COUNT(*) AS common_bookmarks,
               SUM(CASE WHEN u1.mbti = u2.mbti THEN :sameMbtiWeight ELSE :diffMbtiWeight END) AS weighted_co
        FROM bookmarks b1 
        JOIN bookmarks b2 ON b1.user_id = b2.user_id AND b1.place_id != b2.place_id
        JOIN places p1 ON b1.place_id = p1.id 
        JOIN places p2 ON b2.place_id = p2.id
        JOIN users u1 ON b1.user_id = u1.id
        JOIN users u2 ON b2.user_id = u2.id  
        WHERE b1.created_at >= :timeDecayStart OR b2.created_at >= :timeDecayStart
        GROUP BY p1.id, p2.id
        HAVING COUNT(*) >= 2
    """, nativeQuery = true)
    fun findBookmarkCoOccurrences(
        @Param("sameMbtiWeight") sameMbtiWeight: Double,
        @Param("diffMbtiWeight") diffMbtiWeight: Double,
        @Param("timeDecayStart") timeDecayStart: LocalDateTime
    ): List<Map<String, Any>>
}

@Repository
interface PlaceSimilarityTopKRepository : JpaRepository<PlaceSimilarityTopK, PlaceSimilarityTopKId> {
    
    fun findByPlaceIdOrderByRank(placeId: Long): List<PlaceSimilarityTopK>
    
    @Query("SELECT t FROM PlaceSimilarityTopK t WHERE t.placeId IN :placeIds ORDER BY t.placeId, t.rank")
    fun findByPlaceIdInOrderByPlaceIdAndRank(@Param("placeIds") placeIds: List<Long>): List<PlaceSimilarityTopK>
    
    @Modifying
    @Query("DELETE FROM PlaceSimilarityTopK t WHERE t.placeId = :placeId")
    fun deleteByPlaceId(@Param("placeId") placeId: Long)
    
    @Modifying
    @Query("""
        INSERT INTO place_similarity_topk (place_id, neighbor_place_id, rank, jaccard, cosine_bin, co_users, updated_at)
        SELECT :placeId, neighbor_id, ROW_NUMBER() OVER (ORDER BY score DESC) as rank, 
               jaccard, cosine_bin, co_users, :updatedAt
        FROM (
            SELECT 
                CASE WHEN s.place_id1 = :placeId THEN s.place_id2 ELSE s.place_id1 END as neighbor_id,
                s.jaccard, s.cosine_bin, s.co_users,
                (:jaccardWeight * s.jaccard + :cosineWeight * s.cosine_bin) as score
            FROM place_similarity s
            WHERE (s.place_id1 = :placeId OR s.place_id2 = :placeId)
            AND (s.jaccard > 0 OR s.cosine_bin > 0)
            ORDER BY score DESC
            LIMIT :topK
        ) ranked
        ON CONFLICT (place_id, neighbor_place_id) 
        DO UPDATE SET 
            rank = EXCLUDED.rank,
            jaccard = EXCLUDED.jaccard,
            cosine_bin = EXCLUDED.cosine_bin,
            co_users = EXCLUDED.co_users,
            updated_at = EXCLUDED.updated_at
    """, nativeQuery = true)
    fun refreshTopKForPlace(
        @Param("placeId") placeId: Long,
        @Param("topK") topK: Int,
        @Param("jaccardWeight") jaccardWeight: Double,
        @Param("cosineWeight") cosineWeight: Double,
        @Param("updatedAt") updatedAt: LocalDateTime
    )
}