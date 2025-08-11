package com.mohe.spring.repository

import com.mohe.spring.entity.KeywordCatalog
import org.springframework.data.jpa.repository.JpaRepository
import org.springframework.data.jpa.repository.Query
import org.springframework.data.repository.query.Param
import org.springframework.stereotype.Repository

@Repository
interface KeywordCatalogRepository : JpaRepository<KeywordCatalog, Long> {
    
    /**
     * Find a keyword by its name
     */
    fun findByKeyword(keyword: String): KeywordCatalog?
    
    /**
     * Find all keywords in a specific category
     */
    fun findByCategory(category: String): List<KeywordCatalog>
    
    /**
     * Find keywords by their vector position range
     */
    fun findByVectorPositionBetween(start: Int, end: Int): List<KeywordCatalog>
    
    /**
     * Find keywords that contain specific related groups
     */
    @Query("SELECT kc FROM KeywordCatalog kc WHERE :group = ANY(kc.relatedGroups)")
    fun findByRelatedGroupsContaining(@Param("group") group: String): List<KeywordCatalog>
    
    /**
     * Get all keywords ordered by vector position for vector creation
     */
    fun findAllByOrderByVectorPosition(): List<KeywordCatalog>
    
    /**
     * Check if a keyword exists
     */
    fun existsByKeyword(keyword: String): Boolean
    
    /**
     * Get the maximum vector position (should be 99 for 100-dimensional vector)
     */
    @Query("SELECT MAX(kc.vectorPosition) FROM KeywordCatalog kc")
    fun getMaxVectorPosition(): Int?
    
    /**
     * Find keywords by partial name match (for fuzzy matching)
     */
    @Query("SELECT kc FROM KeywordCatalog kc WHERE LOWER(kc.keyword) LIKE LOWER(CONCAT('%', :keyword, '%'))")
    fun findByKeywordContainingIgnoreCase(@Param("keyword") keyword: String): List<KeywordCatalog>
}