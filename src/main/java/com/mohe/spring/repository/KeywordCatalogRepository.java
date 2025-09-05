package com.mohe.spring.repository;

import com.mohe.spring.entity.KeywordCatalog;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

@Repository
public interface KeywordCatalogRepository extends JpaRepository<KeywordCatalog, Long> {
    
    /**
     * Find a keyword by its name
     */
    Optional<KeywordCatalog> findByKeyword(String keyword);
    
    /**
     * Find all keywords in a specific category
     */
    List<KeywordCatalog> findByCategory(String category);
    
    /**
     * Find keywords by their vector position range
     */
    List<KeywordCatalog> findByVectorPositionBetween(int start, int end);
    
    /**
     * Find keywords that contain specific related groups
     */
    @Query("SELECT kc FROM KeywordCatalog kc WHERE kc.relatedGroups LIKE CONCAT('%', :group, '%')")
    List<KeywordCatalog> findByRelatedGroupsContaining(@Param("group") String group);
    
    /**
     * Get all keywords ordered by vector position for vector creation
     */
    List<KeywordCatalog> findAllByOrderByVectorPosition();
    
    /**
     * Check if a keyword exists
     */
    boolean existsByKeyword(String keyword);
    
    /**
     * Get the maximum vector position (should be 99 for 100-dimensional vector)
     */
    @Query("SELECT MAX(kc.vectorPosition) FROM KeywordCatalog kc")
    Optional<Integer> getMaxVectorPosition();
    
    /**
     * Find keywords by partial name match (for fuzzy matching)
     */
    @Query("SELECT kc FROM KeywordCatalog kc WHERE LOWER(kc.keyword) LIKE LOWER(CONCAT('%', :keyword, '%'))")
    List<KeywordCatalog> findByKeywordContainingIgnoreCase(@Param("keyword") String keyword);
}