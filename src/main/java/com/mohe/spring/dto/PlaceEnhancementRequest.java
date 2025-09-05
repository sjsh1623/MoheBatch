package com.mohe.spring.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotNull;
import java.util.List;

public class PlaceEnhancementRequest {
    
    @JsonProperty("place_id")
    @NotNull(message = "장소 ID는 필수입니다")
    private Long placeId;
    
    @JsonProperty("enable_mbti_descriptions")
    private Boolean enableMbtiDescriptions = true;
    
    @JsonProperty("enable_keyword_extraction")
    private Boolean enableKeywordExtraction = true;
    
    @JsonProperty("enable_image_optimization")
    private Boolean enableImageOptimization = false;
    
    @JsonProperty("additional_keywords")
    private List<String> additionalKeywords;
    
    @JsonProperty("force_regenerate")
    private Boolean forceRegenerate = false;
    
    // Default constructor
    public PlaceEnhancementRequest() {}
    
    // Constructor with fields
    public PlaceEnhancementRequest(Long placeId, Boolean enableMbtiDescriptions,
                                  Boolean enableKeywordExtraction, Boolean enableImageOptimization,
                                  List<String> additionalKeywords, Boolean forceRegenerate) {
        this.placeId = placeId;
        this.enableMbtiDescriptions = enableMbtiDescriptions;
        this.enableKeywordExtraction = enableKeywordExtraction;
        this.enableImageOptimization = enableImageOptimization;
        this.additionalKeywords = additionalKeywords;
        this.forceRegenerate = forceRegenerate;
    }
    
    // Getters and setters
    public Long getPlaceId() {
        return placeId;
    }
    
    public void setPlaceId(Long placeId) {
        this.placeId = placeId;
    }
    
    public Boolean getEnableMbtiDescriptions() {
        return enableMbtiDescriptions;
    }
    
    public void setEnableMbtiDescriptions(Boolean enableMbtiDescriptions) {
        this.enableMbtiDescriptions = enableMbtiDescriptions;
    }
    
    public Boolean getEnableKeywordExtraction() {
        return enableKeywordExtraction;
    }
    
    public void setEnableKeywordExtraction(Boolean enableKeywordExtraction) {
        this.enableKeywordExtraction = enableKeywordExtraction;
    }
    
    public Boolean getEnableImageOptimization() {
        return enableImageOptimization;
    }
    
    public void setEnableImageOptimization(Boolean enableImageOptimization) {
        this.enableImageOptimization = enableImageOptimization;
    }
    
    public List<String> getAdditionalKeywords() {
        return additionalKeywords;
    }
    
    public void setAdditionalKeywords(List<String> additionalKeywords) {
        this.additionalKeywords = additionalKeywords;
    }
    
    public Boolean getForceRegenerate() {
        return forceRegenerate;
    }
    
    public void setForceRegenerate(Boolean forceRegenerate) {
        this.forceRegenerate = forceRegenerate;
    }
}