package com.mohe.spring.repository;

import com.mohe.spring.entity.Place;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Optional;

@Repository
public interface PlaceRepository extends JpaRepository<Place, Long> {
    
    Page<Place> findByCategory(String category, Pageable pageable);
    
    @Query("SELECT p FROM Place p WHERE p.rating >= :minRating ORDER BY p.rating DESC, p.reviewCount DESC")
    Page<Place> findTopRatedPlaces(@Param("minRating") Double minRating, Pageable pageable);
    
    // Removed problematic query method
    
    @Query("SELECT p FROM Place p ORDER BY p.popularity DESC, p.rating DESC")
    Page<Place> findPopularPlaces(Pageable pageable);
    
    @Query("SELECT p FROM Place p WHERE " +
           "LOWER(p.name) LIKE LOWER(CONCAT('%', :query, '%')) OR " +
           "LOWER(p.description) LIKE LOWER(CONCAT('%', :query, '%')) OR " +
           "LOWER(p.address) LIKE LOWER(CONCAT('%', :query, '%'))")
    Page<Place> searchPlaces(@Param("query") String query, Pageable pageable);
    
    @Query("SELECT p FROM Place p WHERE " +
           "(:category IS NULL OR p.category = :category) AND " +
           "(:location IS NULL OR LOWER(p.address) LIKE LOWER(CONCAT('%', :location, '%')))")
    Page<Place> findPlacesWithFilters(
        @Param("category") String category,
        @Param("location") String location,
        Pageable pageable
    );
    
    @Query("SELECT p FROM Place p WHERE p.latitude IS NOT NULL AND p.longitude IS NOT NULL")
    List<Place> findPlacesWithLocation();
    
    @Query("SELECT DISTINCT p.category FROM Place p WHERE p.category IS NOT NULL")
    List<String> findAllCategories();
    
    // New methods for enhanced place management
    Optional<Place> findByNaverPlaceId(String naverPlaceId);
    
    Optional<Place> findByGooglePlaceId(String googlePlaceId);
    
    Optional<Place> findByName(String name);
    
    @Query("""
        SELECT p FROM Place p 
        WHERE p.name = :name 
        AND ABS(p.latitude - :latitude) < :radius 
        AND ABS(p.longitude - :longitude) < :radius
        ORDER BY (
            ABS(p.latitude - :latitude) + ABS(p.longitude - :longitude)
        ) ASC
    """)
    Optional<Place> findSimilarPlace(
        @Param("name") String name,
        @Param("latitude") BigDecimal latitude,
        @Param("longitude") BigDecimal longitude,
        @Param("radius") BigDecimal radius
    );
    
    // New methods for age-based filtering and dynamic fetching
    @Query("""
        SELECT COUNT(*) FROM Place p 
        WHERE (p.rating >= 0.0 OR p.rating IS NULL)
    """)
    long countRecommendablePlaces();
    
    @Query("""
        SELECT COUNT(*) FROM Place p 
        WHERE (p.rating >= 0.0 OR p.rating IS NULL)
        AND (:category IS NULL OR p.category = :category)
    """)
    long countRecommendablePlacesByCategory(@Param("category") String category);
    
    @Query("""
        SELECT p FROM Place p 
        WHERE (p.rating >= 0.0 OR p.rating IS NULL)
        ORDER BY p.rating DESC, p.name ASC
    """)
    Page<Place> findRecommendablePlaces(Pageable pageable);
    
    @Query("""
        SELECT p FROM Place p 
        WHERE p.shouldRecheckRating = true 
        AND p.lastRatingCheck < :recheckThreshold
        ORDER BY p.lastRatingCheck ASC NULLS FIRST
    """)
    Page<Place> findPlacesNeedingRatingRecheck(
        @Param("recheckThreshold") OffsetDateTime recheckThreshold,
        Pageable pageable
    );

    @Query("SELECT p FROM Place p WHERE p.createdAt < :oldDate AND p.rating < :ratingThreshold")
    List<Place> findOldLowRatedPlaces(
        @Param("oldDate") OffsetDateTime oldDate,
        @Param("ratingThreshold") BigDecimal ratingThreshold
    );

    @Query("SELECT p FROM Place p WHERE p.latitude IS NULL OR p.longitude IS NULL")
    List<Place> findPlacesWithoutCoordinates();
    
    List<Place> findByLatitudeBetweenAndLongitudeBetween(
        BigDecimal minLatitude,
        BigDecimal maxLatitude, 
        BigDecimal minLongitude,
        BigDecimal maxLongitude
    );

    @Query(value = """
        SELECT p.* FROM places p
        WHERE p.latitude IS NOT NULL AND p.longitude IS NOT NULL
        AND (
            6371 * acos(
                cos(radians(:latitude)) * cos(radians(CAST(p.latitude AS DOUBLE PRECISION))) * 
                cos(radians(CAST(p.longitude AS DOUBLE PRECISION)) - radians(:longitude)) + 
                sin(radians(:latitude)) * sin(radians(CAST(p.latitude AS DOUBLE PRECISION)))
            )
        ) <= :distance
        ORDER BY p.review_count DESC, p.rating DESC
        LIMIT 20
    """, nativeQuery = true)
    List<Place> findPopularPlaces(
        @Param("latitude") Double latitude,
        @Param("longitude") Double longitude,
        @Param("distance") Double distance
    );
    
    /**
     * Find places near location that match time-based preferences
     */
    @Query(value = """
        SELECT p.* FROM places p
        WHERE p.latitude IS NOT NULL 
        AND p.longitude IS NOT NULL
        AND (
            ABS(CAST(p.latitude AS DOUBLE PRECISION) - :latitude) * 111000 + 
            ABS(CAST(p.longitude AS DOUBLE PRECISION) - :longitude) * 111000 * COS(RADIANS(:latitude))
        ) <= :distance
        AND (
            :categories IS NULL OR
            EXISTS (
                SELECT 1 FROM unnest(ARRAY[:categories]) AS cat(category)
                WHERE LOWER(p.category) LIKE LOWER('%' || cat.category || '%')
            )
        )
        AND (p.rating >= 3.0 OR p.is_new_place = true)
        ORDER BY p.rating DESC, p.review_count DESC
        LIMIT :#{#pageable.pageSize}
    """, nativeQuery = true)
    List<Place> findNearbyPlacesByTimePreference(
        @Param("latitude") BigDecimal latitude,
        @Param("longitude") BigDecimal longitude,
        @Param("categories") String[] categories,
        @Param("distance") Double distance,
        Pageable pageable
    );
    
    /**
     * Find places that match time-based preferences (no location filter)
     */
    @Query("""
        SELECT p FROM Place p
        WHERE LOWER(p.category) IN :categories
        AND (p.rating >= 3.0 OR p.isNewPlace = true)
        ORDER BY p.rating DESC, p.reviewCount DESC
    """)
    List<Place> findPlacesByTimePreference(
        @Param("categories") List<String> categories,
        Pageable pageable
    );

    /**
     * Find nearby places ordered by distance (for LLM recommendations)
     */
    @Query(value = """
        SELECT p.* FROM places p
        WHERE p.latitude IS NOT NULL AND p.longitude IS NOT NULL
        AND (p.rating >= 3.0 OR p.is_new_place = true)
        AND (
            6371 * acos(
                cos(radians(:latitude)) * cos(radians(CAST(p.latitude AS DOUBLE PRECISION))) *
                cos(radians(CAST(p.longitude AS DOUBLE PRECISION)) - radians(:longitude)) +
                sin(radians(:latitude)) * sin(radians(CAST(p.latitude AS DOUBLE PRECISION)))
            )
        ) <= :distance
        ORDER BY (
            6371 * acos(
                cos(radians(:latitude)) * cos(radians(CAST(p.latitude AS DOUBLE PRECISION))) *
                cos(radians(CAST(p.longitude AS DOUBLE PRECISION)) - radians(:longitude)) +
                sin(radians(:latitude)) * sin(radians(CAST(p.latitude AS DOUBLE PRECISION)))
            )
        ) ASC
        LIMIT :limit
    """, nativeQuery = true)
    List<Place> findNearbyPlacesForLLM(
        @Param("latitude") Double latitude,
        @Param("longitude") Double longitude,
        @Param("distance") Double distance,
        @Param("limit") int limit
    );

    /**
     * Find general places for LLM recommendations (when no location provided)
     */
    @Query("""
        SELECT p FROM Place p
        WHERE (p.rating >= 3.0 OR p.isNewPlace = true)
        ORDER BY p.rating DESC, p.reviewCount DESC
    """)
    List<Place> findGeneralPlacesForLLM(Pageable pageable);

    /**
     * 이미지가 없는 장소들을 찾기 (배치 이미지 업데이트용)
     */
    @Query("""
        SELECT p FROM Place p
        WHERE p.id NOT IN (
            SELECT DISTINCT pi.place.id FROM PlaceImage pi
        )
        ORDER BY p.rating DESC NULLS LAST, p.reviewCount DESC NULLS LAST
    """)
    List<Place> findPlacesWithoutImages();
}