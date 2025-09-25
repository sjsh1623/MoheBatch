package com.mohe.spring.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotNull;
import java.util.List;

public class PlaceOptimizationRequest {
    
    @JsonProperty("place_ids")
    @NotNull(message = "장소 ID 목록은 필수입니다")
    private List<Long> placeIds;
    
    @JsonProperty("optimization_type")
    private String optimizationType = "FULL"; // FULL, IMAGES_ONLY, KEYWORDS_ONLY, SIMILARITY_ONLY
    
    @JsonProperty("batch_size")
    private Integer batchSize = 10;
    
    @JsonProperty("async_processing")
    private Boolean asyncProcessing = true;
    
    @JsonProperty("force_recalculation")
    private Boolean forceRecalculation = false;
    
    // Default constructor
    public PlaceOptimizationRequest() {}
    
    // Constructor with fields
    public PlaceOptimizationRequest(List<Long> placeIds, String optimizationType,
                                   Integer batchSize, Boolean asyncProcessing,
                                   Boolean forceRecalculation) {
        this.placeIds = placeIds;
        this.optimizationType = optimizationType;
        this.batchSize = batchSize;
        this.asyncProcessing = asyncProcessing;
        this.forceRecalculation = forceRecalculation;
    }
    
    // Getters and setters
    public List<Long> getPlaceIds() {
        return placeIds;
    }
    
    public void setPlaceIds(List<Long> placeIds) {
        this.placeIds = placeIds;
    }
    
    public String getOptimizationType() {
        return optimizationType;
    }
    
    public void setOptimizationType(String optimizationType) {
        this.optimizationType = optimizationType;
    }
    
    public Integer getBatchSize() {
        return batchSize;
    }
    
    public void setBatchSize(Integer batchSize) {
        this.batchSize = batchSize;
    }
    
    public Boolean getAsyncProcessing() {
        return asyncProcessing;
    }
    
    public void setAsyncProcessing(Boolean asyncProcessing) {
        this.asyncProcessing = asyncProcessing;
    }
    
    public Boolean getForceRecalculation() {
        return forceRecalculation;
    }
    
    public void setForceRecalculation(Boolean forceRecalculation) {
        this.forceRecalculation = forceRecalculation;
    }
}