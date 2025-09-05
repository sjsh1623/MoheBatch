package com.mohe.spring.repository;

import com.mohe.spring.entity.PlaceSimilarity;
import com.mohe.spring.entity.PlaceSimilarityId;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;

@Repository
public interface PlaceSimilarityRepository extends JpaRepository<PlaceSimilarity, PlaceSimilarityId> {
    
    @Query("""
        SELECT s FROM PlaceSimilarity s 
        WHERE s.placeId1 = :placeId OR s.placeId2 = :placeId
        ORDER BY (s.jaccard + s.cosineBin) DESC
    """)
    List<PlaceSimilarity> findSimilaritiesByPlaceId(@Param("placeId") Long placeId);
    
    @Modifying
    @Query(value = """
        INSERT INTO place_similarity (place_id1, place_id2, jaccard, cosine_bin, co_users, updated_at)
        VALUES (LEAST(:placeId1, :placeId2), GREATEST(:placeId1, :placeId2), :jaccard, :cosineBin, :coUsers, :updatedAt)
        ON CONFLICT (place_id1, place_id2) 
        DO UPDATE SET 
            jaccard = EXCLUDED.jaccard,
            cosine_bin = EXCLUDED.cosine_bin,
            co_users = EXCLUDED.co_users,
            updated_at = EXCLUDED.updated_at
    """, nativeQuery = true)
    void upsertSimilarity(
        @Param("placeId1") Long placeId1,
        @Param("placeId2") Long placeId2,
        @Param("jaccard") BigDecimal jaccard,
        @Param("cosineBin") BigDecimal cosineBin,
        @Param("coUsers") Integer coUsers,
        @Param("updatedAt") LocalDateTime updatedAt
    );
    
    @Query(value = """
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
    List<Map<String, Object>> findBookmarkCoOccurrences(
        @Param("sameMbtiWeight") Double sameMbtiWeight,
        @Param("diffMbtiWeight") Double diffMbtiWeight,
        @Param("timeDecayStart") LocalDateTime timeDecayStart
    );
}