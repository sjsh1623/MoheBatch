package com.mohe.spring.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.DecimalMax;
import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;

public class VectorSimilarityRequest {
    
    @JsonProperty("user_id")
    private Long userId;
    
    @JsonProperty("place_id")
    private Long placeId;
    
    @JsonProperty("similarity_threshold")
    @DecimalMin(value = "0.0", message = "유사도 임계값은 0.0 이상이어야 합니다")
    @DecimalMax(value = "1.0", message = "유사도 임계값은 1.0 이하여야 합니다")
    private Double similarityThreshold = 0.1;
    
    @JsonProperty("limit")
    @Min(value = 1, message = "결과 수는 1 이상이어야 합니다")
    @Max(value = 100, message = "결과 수는 100 이하여야 합니다")
    private Integer limit = 20;
    
    @JsonProperty("include_explanation")
    private Boolean includeExplanation = false;
    
    @JsonProperty("vector_type")
    private String vectorType = "COMBINED"; // USER_PREFERENCE, PLACE_DESCRIPTION, COMBINED
    
    // Default constructor
    public VectorSimilarityRequest() {}
    
    // Constructor with fields
    public VectorSimilarityRequest(Long userId, Long placeId, Double similarityThreshold,
                                  Integer limit, Boolean includeExplanation, String vectorType) {
        this.userId = userId;
        this.placeId = placeId;
        this.similarityThreshold = similarityThreshold;
        this.limit = limit;
        this.includeExplanation = includeExplanation;
        this.vectorType = vectorType;
    }
    
    // Getters and setters
    public Long getUserId() {
        return userId;
    }
    
    public void setUserId(Long userId) {
        this.userId = userId;
    }
    
    public Long getPlaceId() {
        return placeId;
    }
    
    public void setPlaceId(Long placeId) {
        this.placeId = placeId;
    }
    
    public Double getSimilarityThreshold() {
        return similarityThreshold;
    }
    
    public void setSimilarityThreshold(Double similarityThreshold) {
        this.similarityThreshold = similarityThreshold;
    }
    
    public Integer getLimit() {
        return limit;
    }
    
    public void setLimit(Integer limit) {
        this.limit = limit;
    }
    
    public Boolean getIncludeExplanation() {
        return includeExplanation;
    }
    
    public void setIncludeExplanation(Boolean includeExplanation) {
        this.includeExplanation = includeExplanation;
    }
    
    public String getVectorType() {
        return vectorType;
    }
    
    public void setVectorType(String vectorType) {
        this.vectorType = vectorType;
    }
}