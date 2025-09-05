package com.mohe.spring.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import java.util.Map;

public class PlaceEnhancementResponse {
    
    @JsonProperty("place_id")
    private Long placeId;
    
    @JsonProperty("enhancement_status")
    private String enhancementStatus;
    
    @JsonProperty("mbti_descriptions_generated")
    private Boolean mbtiDescriptionsGenerated;
    
    @JsonProperty("keywords_extracted")
    private List<String> keywordsExtracted;
    
    @JsonProperty("images_optimized")
    private Integer imagesOptimized;
    
    @JsonProperty("processing_time_ms")
    private Long processingTimeMs;
    
    @JsonProperty("errors")
    private List<String> errors;
    
    @JsonProperty("metadata")
    private Map<String, Object> metadata;
    
    // Default constructor
    public PlaceEnhancementResponse() {}
    
    // Constructor with fields
    public PlaceEnhancementResponse(Long placeId, String enhancementStatus,
                                   Boolean mbtiDescriptionsGenerated, List<String> keywordsExtracted,
                                   Integer imagesOptimized, Long processingTimeMs,
                                   List<String> errors, Map<String, Object> metadata) {
        this.placeId = placeId;
        this.enhancementStatus = enhancementStatus;
        this.mbtiDescriptionsGenerated = mbtiDescriptionsGenerated;
        this.keywordsExtracted = keywordsExtracted;
        this.imagesOptimized = imagesOptimized;
        this.processingTimeMs = processingTimeMs;
        this.errors = errors;
        this.metadata = metadata;
    }
    
    // Static factory methods
    public static PlaceEnhancementResponse success(Long placeId, Boolean mbtiGenerated,
                                                  List<String> keywords, Integer imagesOptimized,
                                                  Long processingTime) {
        return new PlaceEnhancementResponse(placeId, "SUCCESS", mbtiGenerated, keywords,
                                          imagesOptimized, processingTime, null, null);
    }
    
    public static PlaceEnhancementResponse error(Long placeId, List<String> errors) {
        return new PlaceEnhancementResponse(placeId, "ERROR", false, null, 0, 0L, errors, null);
    }
    
    // Getters and setters
    public Long getPlaceId() {
        return placeId;
    }
    
    public void setPlaceId(Long placeId) {
        this.placeId = placeId;
    }
    
    public String getEnhancementStatus() {
        return enhancementStatus;
    }
    
    public void setEnhancementStatus(String enhancementStatus) {
        this.enhancementStatus = enhancementStatus;
    }
    
    public Boolean getMbtiDescriptionsGenerated() {
        return mbtiDescriptionsGenerated;
    }
    
    public void setMbtiDescriptionsGenerated(Boolean mbtiDescriptionsGenerated) {
        this.mbtiDescriptionsGenerated = mbtiDescriptionsGenerated;
    }
    
    public List<String> getKeywordsExtracted() {
        return keywordsExtracted;
    }
    
    public void setKeywordsExtracted(List<String> keywordsExtracted) {
        this.keywordsExtracted = keywordsExtracted;
    }
    
    public Integer getImagesOptimized() {
        return imagesOptimized;
    }
    
    public void setImagesOptimized(Integer imagesOptimized) {
        this.imagesOptimized = imagesOptimized;
    }
    
    public Long getProcessingTimeMs() {
        return processingTimeMs;
    }
    
    public void setProcessingTimeMs(Long processingTimeMs) {
        this.processingTimeMs = processingTimeMs;
    }
    
    public List<String> getErrors() {
        return errors;
    }
    
    public void setErrors(List<String> errors) {
        this.errors = errors;
    }
    
    public Map<String, Object> getMetadata() {
        return metadata;
    }
    
    public void setMetadata(Map<String, Object> metadata) {
        this.metadata = metadata;
    }
}