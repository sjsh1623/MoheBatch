package com.mohe.spring.repository

import com.mohe.spring.entity.Place
import org.springframework.data.domain.Page
import org.springframework.data.domain.Pageable
import org.springframework.data.jpa.repository.JpaRepository
import org.springframework.data.jpa.repository.Query
import org.springframework.data.repository.query.Param
import org.springframework.stereotype.Repository
import java.math.BigDecimal
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.OffsetDateTime
import java.util.*

@Repository
interface PlaceRepository : JpaRepository<Place, Long> {
    
    fun findByCategory(category: String, pageable: Pageable): Page<Place>
    
    @Query("SELECT p FROM Place p WHERE p.rating >= :minRating ORDER BY p.rating DESC, p.reviewCount DESC")
    fun findTopRatedPlaces(@Param("minRating") minRating: Double, pageable: Pageable): Page<Place>
    
    @Query("SELECT p FROM Place p ORDER BY p.popularity DESC, p.rating DESC")
    fun findPopularPlaces(pageable: Pageable): Page<Place>
    
    @Query("SELECT p FROM Place p WHERE " +
           "LOWER(p.title) LIKE LOWER(CONCAT('%', :query, '%')) OR " +
           "LOWER(p.name) LIKE LOWER(CONCAT('%', :query, '%')) OR " +
           "LOWER(p.description) LIKE LOWER(CONCAT('%', :query, '%')) OR " +
           "LOWER(p.location) LIKE LOWER(CONCAT('%', :query, '%'))")
    fun searchPlaces(@Param("query") query: String, pageable: Pageable): Page<Place>
    
    @Query("SELECT p FROM Place p WHERE " +
           "(:category IS NULL OR p.category = :category) AND " +
           "(:location IS NULL OR LOWER(p.location) LIKE LOWER(CONCAT('%', :location, '%')))")
    fun findPlacesWithFilters(
        @Param("category") category: String?,
        @Param("location") location: String?,
        pageable: Pageable
    ): Page<Place>
    
    @Query("SELECT p FROM Place p WHERE p.latitude IS NOT NULL AND p.longitude IS NOT NULL")
    fun findPlacesWithLocation(): List<Place>
    
    @Query("SELECT DISTINCT p.category FROM Place p WHERE p.category IS NOT NULL")
    fun findAllCategories(): List<String>
    
    // New methods for enhanced place management
    fun findByNaverPlaceId(naverPlaceId: String): Place?
    
    fun findByGooglePlaceId(googlePlaceId: String): Place?
    
    fun findByName(name: String): Optional<Place>
    
    @Query("""
        SELECT p FROM Place p 
        WHERE p.name = :name 
        AND ABS(p.latitude - :latitude) < :radius 
        AND ABS(p.longitude - :longitude) < :radius
        ORDER BY (
            ABS(p.latitude - :latitude) + ABS(p.longitude - :longitude)
        ) ASC
    """)
    fun findSimilarPlace(
        @Param("name") name: String,
        @Param("latitude") latitude: java.math.BigDecimal,
        @Param("longitude") longitude: java.math.BigDecimal,
        @Param("radius") radius: java.math.BigDecimal
    ): Place?
    
    // New methods for age-based filtering and dynamic fetching
    @Query("""
        SELECT COUNT(*) FROM Place p 
        WHERE (p.rating >= 3.0 OR p.isNewPlace = true OR p.openedDate > :sixMonthsAgo)
    """)
    fun countRecommendablePlaces(@Param("sixMonthsAgo") sixMonthsAgo: LocalDate = LocalDate.now().minusMonths(6)): Long
    
    @Query("""
        SELECT COUNT(*) FROM Place p 
        WHERE (p.rating >= 3.0 OR p.isNewPlace = true OR p.openedDate > :sixMonthsAgo)
        AND (:category IS NULL OR p.category = :category)
    """)
    fun countRecommendablePlacesByCategory(
        @Param("category") category: String,
        @Param("sixMonthsAgo") sixMonthsAgo: LocalDate = LocalDate.now().minusMonths(6)
    ): Long
    
    @Query("""
        SELECT p FROM Place p 
        WHERE (p.rating >= 3.0 OR p.isNewPlace = true OR p.openedDate > :sixMonthsAgo)
        ORDER BY p.rating DESC, p.firstSeenAt DESC
    """)
    fun findRecommendablePlaces(
        pageable: Pageable,
        @Param("sixMonthsAgo") sixMonthsAgo: LocalDate = LocalDate.now().minusMonths(6)
    ): Page<Place>
    
    @Query("""
        SELECT p FROM Place p 
        WHERE p.shouldRecheckRating = true 
        AND p.lastRatingCheck < :recheckThreshold
        ORDER BY p.lastRatingCheck ASC NULLS FIRST
    """)
    fun findPlacesNeedingRatingRecheck(
        @Param("recheckThreshold") recheckThreshold: OffsetDateTime,
        pageable: Pageable
    ): Page<Place>

    @Query("SELECT p FROM Place p WHERE p.createdAt < :oldDate AND p.rating < :ratingThreshold")
    fun findOldLowRatedPlaces(
        @Param("oldDate") oldDate: LocalDateTime,
        @Param("ratingThreshold") ratingThreshold: BigDecimal
    ): List<Place>

    @Query("SELECT p FROM Place p WHERE p.latitude IS NULL OR p.longitude IS NULL")
    fun findPlacesWithoutCoordinates(): List<Place>
    
    fun findByLatitudeBetweenAndLongitudeBetween(
        minLatitude: BigDecimal,
        maxLatitude: BigDecimal, 
        minLongitude: BigDecimal,
        maxLongitude: BigDecimal
    ): List<Place>

    @Query(value = """
        SELECT p.* FROM places p
        WHERE ST_DWithin(
            p.location_geom,
            ST_MakePoint(:longitude, :latitude),
            :distance
        )
        ORDER BY p.bookmark_count DESC
        LIMIT 20
    """, nativeQuery = true)
    fun findPopularPlaces(
        @Param("latitude") latitude: Double,
        @Param("longitude") longitude: Double,
        @Param("distance") distance: Double
    ): List<Place>
}