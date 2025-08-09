package com.mohe.spring.repository

import com.mohe.spring.entity.Place
import org.springframework.data.domain.Page
import org.springframework.data.domain.Pageable
import org.springframework.data.jpa.repository.JpaRepository
import org.springframework.data.jpa.repository.Query
import org.springframework.data.repository.query.Param
import org.springframework.stereotype.Repository
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
}