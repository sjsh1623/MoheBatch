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
     * Check if a keyword exists
     */
    boolean existsByKeyword(String keyword);
    
    /**
     * Find keywords by partial name match (for fuzzy matching)
     */
    @Query("SELECT kc FROM KeywordCatalog kc WHERE LOWER(kc.keyword) LIKE LOWER(CONCAT('%', :keyword, '%'))")
    List<KeywordCatalog> findByKeywordContainingIgnoreCase(@Param("keyword") String keyword);
}