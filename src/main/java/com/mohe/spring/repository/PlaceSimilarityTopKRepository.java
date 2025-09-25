package com.mohe.spring.repository;

import com.mohe.spring.entity.PlaceSimilarityTopK;
import com.mohe.spring.entity.PlaceSimilarityTopKId;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.time.LocalDateTime;
import java.util.List;

@Repository
public interface PlaceSimilarityTopKRepository extends JpaRepository<PlaceSimilarityTopK, PlaceSimilarityTopKId> {
    
    List<PlaceSimilarityTopK> findByPlaceIdOrderByRank(Long placeId);
    
    @Query("SELECT t FROM PlaceSimilarityTopK t WHERE t.placeId IN :placeIds ORDER BY t.placeId, t.rank")
    List<PlaceSimilarityTopK> findByPlaceIdInOrderByPlaceIdAndRank(@Param("placeIds") List<Long> placeIds);
    
    @Modifying
    @Query("DELETE FROM PlaceSimilarityTopK t WHERE t.placeId = :placeId")
    void deleteByPlaceId(@Param("placeId") Long placeId);
    
    @Modifying
    @Query(value = """
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
    void refreshTopKForPlace(
        @Param("placeId") Long placeId,
        @Param("topK") Integer topK,
        @Param("jaccardWeight") Double jaccardWeight,
        @Param("cosineWeight") Double cosineWeight,
        @Param("updatedAt") LocalDateTime updatedAt
    );
}