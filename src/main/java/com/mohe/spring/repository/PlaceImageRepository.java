package com.mohe.spring.repository;

import com.mohe.spring.entity.ImageSource;
import com.mohe.spring.entity.ImageType;
import com.mohe.spring.entity.PlaceImage;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

@Repository
public interface PlaceImageRepository extends JpaRepository<PlaceImage, Long> {
    
    /**
     * Find all images for a specific place, ordered by display order
     */
    List<PlaceImage> findByPlaceIdOrderByDisplayOrderAsc(Long placeId);
    
    /**
     * Find primary image for a place
     */
    Optional<PlaceImage> findByPlaceIdAndIsPrimaryTrue(Long placeId);
    
    /**
     * Find images by place ID and type
     */
    List<PlaceImage> findByPlaceIdAndImageType(Long placeId, ImageType imageType);
    
    /**
     * Find verified images for a place
     */
    List<PlaceImage> findByPlaceIdAndIsVerifiedTrueOrderByDisplayOrderAsc(Long placeId);
    
    /**
     * Count images for a place
     */
    int countByPlaceId(Long placeId);
    
    /**
     * Find places with fewer than minimum required images
     */
    @Query("""
        SELECT DISTINCT pi.place.id 
        FROM PlaceImage pi 
        GROUP BY pi.place.id 
        HAVING COUNT(pi.id) < :minImages
    """)
    List<Long> findPlaceIdsWithInsufficientImages(@Param("minImages") int minImages);
    
    /**
     * Find places with no images at all
     */
    @Query("""
        SELECT p.id FROM Place p 
        WHERE p.id NOT IN (
            SELECT DISTINCT pi.place.id FROM PlaceImage pi
        )
    """)
    List<Long> findPlaceIdsWithNoImages(Pageable pageable);
    
    /**
     * Find images by source for audit purposes
     */
    List<PlaceImage> findBySourceOrderByCreatedAtDesc(ImageSource source, Pageable pageable);
    
    /**
     * Delete all images for a place (cascade cleanup)
     */
    void deleteByPlaceId(Long placeId);
    
    /**
     * Find images that need verification
     */
    List<PlaceImage> findByIsVerifiedFalseOrderByCreatedAtDesc(Pageable pageable);
    
    /**
     * Check if place has primary image
     */
    boolean existsByPlaceIdAndIsPrimaryTrue(Long placeId);
    
    /**
     * Get image counts by type for a place
     */
    @Query("""
        SELECT pi.imageType, COUNT(pi.id)
        FROM PlaceImage pi
        WHERE pi.place.id = :placeId
        GROUP BY pi.imageType
    """)
    List<Object[]> getImageCountsByType(@Param("placeId") Long placeId);

    /**
     * Find AI generated images for a place
     */
    List<PlaceImage> findByPlaceIdAndIsAiGeneratedTrue(Long placeId);

    /**
     * Find all AI generated images
     */
    List<PlaceImage> findByIsAiGeneratedTrueOrderByCreatedAtDesc(Pageable pageable);

    /**
     * Check if place has AI generated images
     */
    boolean existsByPlaceIdAndIsAiGeneratedTrue(Long placeId);

    /**
     * Count AI generated images for a place
     */
    int countByPlaceIdAndIsAiGeneratedTrue(Long placeId);

    /**
     * Find images by AI model
     */
    List<PlaceImage> findByAiModelOrderByCreatedAtDesc(String aiModel, Pageable pageable);
}