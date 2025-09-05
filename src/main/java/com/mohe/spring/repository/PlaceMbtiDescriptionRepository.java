package com.mohe.spring.repository;

import com.mohe.spring.entity.PlaceMbtiDescription;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;

@Repository
public interface PlaceMbtiDescriptionRepository extends JpaRepository<PlaceMbtiDescription, Long> {
    
    Optional<PlaceMbtiDescription> findByPlaceIdAndMbti(Long placeId, String mbti);
    
    List<PlaceMbtiDescription> findByPlaceId(Long placeId);
    
    List<PlaceMbtiDescription> findByMbti(String mbti);
    
    @Query("SELECT p FROM PlaceMbtiDescription p WHERE p.placeId IN :placeIds")
    List<PlaceMbtiDescription> findByPlaceIdIn(@Param("placeIds") List<Long> placeIds);
    
    @Query("SELECT p FROM PlaceMbtiDescription p WHERE p.placeId IN :placeIds AND p.mbti = :mbti")
    List<PlaceMbtiDescription> findByPlaceIdInAndMbti(@Param("placeIds") List<Long> placeIds, @Param("mbti") String mbti);
    
    @Modifying
    @Query(value = """
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
    void upsertMbtiDescription(
        @Param("placeId") Long placeId,
        @Param("mbti") String mbti,
        @Param("description") String description,
        @Param("model") String model,
        @Param("promptHash") String promptHash,
        @Param("updatedAt") LocalDateTime updatedAt
    );
    
    @Query(value = """
        SELECT COUNT(*) FROM place_mbti_descriptions 
        WHERE place_id = :placeId AND updated_at > :since
    """, nativeQuery = true)
    long countRecentDescriptions(@Param("placeId") Long placeId, @Param("since") LocalDateTime since);
    
    @Modifying
    @Query("DELETE FROM PlaceMbtiDescription p WHERE p.updatedAt < :cutoffDate")
    void deleteOldDescriptions(@Param("cutoffDate") LocalDateTime cutoffDate);
}