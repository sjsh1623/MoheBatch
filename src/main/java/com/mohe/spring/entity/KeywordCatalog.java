package com.mohe.spring.entity;

import jakarta.persistence.*;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

@Entity
@Table(name = "keyword_catalog")
public class KeywordCatalog {
    
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id = 0L;
    
    @Column(name = "keyword", nullable = false, unique = true, length = 100)
    private String keyword;
    
    @Column(name = "definition", nullable = false, columnDefinition = "TEXT")
    private String definition;
    
    @Column(name = "category", nullable = false, length = 50)
    private String category;
    
    @Column(name = "related_groups", columnDefinition = "TEXT")
    private String relatedGroups = ""; // Store as comma-separated string
    
    @Column(name = "vector_position", nullable = false)
    private Integer vectorPosition; // Position in the 100-dimensional vector (0-99)
    
    @Column(name = "created_at")
    private LocalDateTime createdAt = LocalDateTime.now();
    
    @Column(name = "updated_at")
    private LocalDateTime updatedAt = LocalDateTime.now();
    
    // Default constructor for JPA
    public KeywordCatalog() {}
    
    // Constructor with required fields
    public KeywordCatalog(String keyword, String definition, String category, Integer vectorPosition) {
        this.keyword = keyword;
        this.definition = definition;
        this.category = category;
        this.vectorPosition = vectorPosition;
    }
    
    /**
     * Get related groups as a list
     */
    public List<String> getRelatedGroupsList() {
        if (relatedGroups == null || relatedGroups.trim().isEmpty()) {
            return Collections.emptyList();
        }
        return Arrays.stream(relatedGroups.split(","))
                .map(String::trim)
                .collect(Collectors.toList());
    }
    
    // Getters and Setters
    public Long getId() {
        return id;
    }
    
    public void setId(Long id) {
        this.id = id;
    }
    
    public String getKeyword() {
        return keyword;
    }
    
    public void setKeyword(String keyword) {
        this.keyword = keyword;
    }
    
    public String getDefinition() {
        return definition;
    }
    
    public void setDefinition(String definition) {
        this.definition = definition;
    }
    
    public String getCategory() {
        return category;
    }
    
    public void setCategory(String category) {
        this.category = category;
    }
    
    public String getRelatedGroups() {
        return relatedGroups;
    }
    
    public void setRelatedGroups(String relatedGroups) {
        this.relatedGroups = relatedGroups;
    }
    
    public Integer getVectorPosition() {
        return vectorPosition;
    }
    
    public void setVectorPosition(Integer vectorPosition) {
        this.vectorPosition = vectorPosition;
    }
    
    public LocalDateTime getCreatedAt() {
        return createdAt;
    }
    
    public void setCreatedAt(LocalDateTime createdAt) {
        this.createdAt = createdAt;
    }
    
    public LocalDateTime getUpdatedAt() {
        return updatedAt;
    }
    
    public void setUpdatedAt(LocalDateTime updatedAt) {
        this.updatedAt = updatedAt;
    }
}