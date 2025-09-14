package com.mohe.spring.entity;

import jakarta.persistence.*;
import java.time.LocalDateTime;

@Entity
@Table(name = "keyword_catalog")
public class KeywordCatalog {
    
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id = 0L;
    
    @Column(name = "keyword", nullable = false, unique = true, length = 100)
    private String keyword;

    @Column(name = "category", nullable = false, length = 50)
    private String category;

    @Column(name = "mbti_weights", columnDefinition = "jsonb")
    private String mbtiWeights = "{}";

    @Column(name = "created_at")
    private LocalDateTime createdAt = LocalDateTime.now();
    
    @Column(name = "updated_at")
    private LocalDateTime updatedAt = LocalDateTime.now();
    
    // Default constructor for JPA
    public KeywordCatalog() {}
    
    // Constructor with required fields
    public KeywordCatalog(String keyword, String category, String mbtiWeights) {
        this.keyword = keyword;
        this.category = category;
        this.mbtiWeights = mbtiWeights;
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
    
    public String getMbtiWeights() {
        return mbtiWeights;
    }

    public void setMbtiWeights(String mbtiWeights) {
        this.mbtiWeights = mbtiWeights;
    }
    
    public String getCategory() {
        return category;
    }
    
    public void setCategory(String category) {
        this.category = category;
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