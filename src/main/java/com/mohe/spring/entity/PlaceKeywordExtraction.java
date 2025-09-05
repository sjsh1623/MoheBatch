package com.mohe.spring.entity;

import jakarta.persistence.*;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Objects;

@Entity
@Table(name = "place_keyword_extractions")
public class PlaceKeywordExtraction {
    
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id = 0L;
    
    @Column(name = "place_id", nullable = false)
    private Long placeId;
    
    @Column(name = "raw_text", nullable = false, columnDefinition = "TEXT")
    private String rawText; // Original place description used for extraction
    
    @Column(name = "model_name", nullable = false, length = 100)
    private String modelName; // e.g., "ollama-openai"
    
    @Column(name = "model_version", nullable = false, length = 50)
    private String modelVersion; // e.g., "llama3.1:latest"
    
    @Column(name = "keyword_vector", nullable = false, columnDefinition = "vector(100)")
    private float[] keywordVector; // pgvector column for similarity search
    
    @Column(name = "selected_keywords", nullable = false, columnDefinition = "JSONB")
    private String selectedKeywords; // JSONB array of {keyword, confidence_score} objects
    
    @Column(name = "extraction_method", length = 50)
    private String extractionMethod = "ollama_llm";
    
    @Column(name = "processing_time_ms")
    private Integer processingTimeMs;
    
    @Column(name = "created_at")
    private LocalDateTime createdAt = LocalDateTime.now();
    
    @Column(name = "updated_at")
    private LocalDateTime updatedAt = LocalDateTime.now();
    
    // Foreign key relationship to Place entity
    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "place_id", insertable = false, updatable = false)
    private Place place;
    
    // Default constructor for JPA
    public PlaceKeywordExtraction() {}
    
    // Constructor with required fields
    public PlaceKeywordExtraction(Long placeId, String rawText, String modelName, String modelVersion,
                                 float[] keywordVector, String selectedKeywords, String promptHash) {
        this.placeId = placeId;
        this.rawText = rawText;
        this.modelName = modelName;
        this.modelVersion = modelVersion;
        this.keywordVector = keywordVector;
        this.selectedKeywords = selectedKeywords;
    }
    
    // Getters and Setters
    public Long getId() {
        return id;
    }
    
    public void setId(Long id) {
        this.id = id;
    }
    
    public Long getPlaceId() {
        return placeId;
    }
    
    public void setPlaceId(Long placeId) {
        this.placeId = placeId;
    }
    
    public String getRawText() {
        return rawText;
    }
    
    public void setRawText(String rawText) {
        this.rawText = rawText;
    }
    
    public String getModelName() {
        return modelName;
    }
    
    public void setModelName(String modelName) {
        this.modelName = modelName;
    }
    
    public String getModelVersion() {
        return modelVersion;
    }
    
    public void setModelVersion(String modelVersion) {
        this.modelVersion = modelVersion;
    }
    
    public float[] getKeywordVector() {
        return keywordVector;
    }
    
    public void setKeywordVector(float[] keywordVector) {
        this.keywordVector = keywordVector;
    }
    
    public String getSelectedKeywords() {
        return selectedKeywords;
    }
    
    public void setSelectedKeywords(String selectedKeywords) {
        this.selectedKeywords = selectedKeywords;
    }
    
    public String getExtractionMethod() {
        return extractionMethod;
    }
    
    public void setExtractionMethod(String extractionMethod) {
        this.extractionMethod = extractionMethod;
    }
    
    public Integer getProcessingTimeMs() {
        return processingTimeMs;
    }
    
    public void setProcessingTimeMs(Integer processingTimeMs) {
        this.processingTimeMs = processingTimeMs;
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
    
    public Place getPlace() {
        return place;
    }
    
    public void setPlace(Place place) {
        this.place = place;
    }
    
    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        PlaceKeywordExtraction that = (PlaceKeywordExtraction) obj;
        return Objects.equals(id, that.id) && Objects.equals(placeId, that.placeId);
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(id, placeId);
    }
    
    @Override
    public String toString() {
        return "PlaceKeywordExtraction(" +
                "id=" + id +
                ", placeId=" + placeId +
                ", modelName='" + modelName + '\'' +
                ", extractionMethod='" + extractionMethod + '\'' +
                ')';
    }
}