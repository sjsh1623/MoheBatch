package com.mohe.spring.repository

import com.mohe.spring.entity.PlaceImage
import com.mohe.spring.entity.ImageType
import com.mohe.spring.entity.ImageSource
import org.springframework.data.domain.Pageable
import org.springframework.data.jpa.repository.JpaRepository
import org.springframework.data.jpa.repository.Query
import org.springframework.data.repository.query.Param
import org.springframework.stereotype.Repository

@Repository
interface PlaceImageRepository : JpaRepository<PlaceImage, Long> {
    
    /**
     * Find all images for a specific place, ordered by display order
     */
    fun findByPlaceIdOrderByDisplayOrderAsc(placeId: Long): List<PlaceImage>
    
    /**
     * Find primary image for a place
     */
    fun findByPlaceIdAndIsPrimaryTrue(placeId: Long): PlaceImage?
    
    /**
     * Find images by place ID and type
     */
    fun findByPlaceIdAndImageType(placeId: Long, imageType: ImageType): List<PlaceImage>
    
    /**
     * Find verified images for a place
     */
    fun findByPlaceIdAndIsVerifiedTrueOrderByDisplayOrderAsc(placeId: Long): List<PlaceImage>
    
    /**
     * Count images for a place
     */
    fun countByPlaceId(placeId: Long): Int
    
    /**
     * Find places with fewer than minimum required images
     */
    @Query("""
        SELECT DISTINCT pi.place.id 
        FROM PlaceImage pi 
        GROUP BY pi.place.id 
        HAVING COUNT(pi.id) < :minImages
    """)
    fun findPlaceIdsWithInsufficientImages(@Param("minImages") minImages: Int): List<Long>
    
    /**
     * Find places with no images at all
     */
    @Query("""
        SELECT p.id FROM Place p 
        WHERE p.id NOT IN (
            SELECT DISTINCT pi.place.id FROM PlaceImage pi
        )
    """)
    fun findPlaceIdsWithNoImages(pageable: Pageable): List<Long>
    
    /**
     * Find images by source for audit purposes
     */
    fun findBySourceOrderByCreatedAtDesc(source: ImageSource, pageable: Pageable): List<PlaceImage>
    
    /**
     * Delete all images for a place (cascade cleanup)
     */
    fun deleteByPlaceId(placeId: Long)
    
    /**
     * Find images that need verification
     */
    fun findByIsVerifiedFalseOrderByCreatedAtDesc(pageable: Pageable): List<PlaceImage>
    
    /**
     * Check if place has primary image
     */
    fun existsByPlaceIdAndIsPrimaryTrue(placeId: Long): Boolean
    
    /**
     * Get image counts by type for a place
     */
    @Query("""
        SELECT pi.imageType, COUNT(pi.id)
        FROM PlaceImage pi 
        WHERE pi.place.id = :placeId 
        GROUP BY pi.imageType
    """)
    fun getImageCountsByType(@Param("placeId") placeId: Long): List<Array<Any>>
}