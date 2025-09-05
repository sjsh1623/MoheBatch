package com.mohe.spring.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;

public class SimilarityRecalculationRequest {
    
    @JsonProperty("place_ids")
    private List<Long> placeIds;
    
    @JsonProperty("user_ids")
    private List<Long> userIds;
    
    @JsonProperty("recalculation_type")
    private String recalculationType = "FULL"; // FULL, INCREMENTAL, TOPK_ONLY, VECTOR_ONLY
    
    @JsonProperty("force_recalculation")
    private Boolean forceRecalculation = false;
    
    @JsonProperty("async_processing")
    private Boolean asyncProcessing = true;
    
    @JsonProperty("batch_size")
    private Integer batchSize = 100;
    
    @JsonProperty("update_topk_cache")
    private Boolean updateTopkCache = true;
    
    @JsonProperty("recalculate_vectors")
    private Boolean recalculateVectors = true;
    
    // Default constructor
    public SimilarityRecalculationRequest() {}
    
    // Constructor with fields
    public SimilarityRecalculationRequest(List<Long> placeIds, List<Long> userIds,
                                         String recalculationType, Boolean forceRecalculation,
                                         Boolean asyncProcessing, Integer batchSize,
                                         Boolean updateTopkCache, Boolean recalculateVectors) {
        this.placeIds = placeIds;
        this.userIds = userIds;
        this.recalculationType = recalculationType;
        this.forceRecalculation = forceRecalculation;
        this.asyncProcessing = asyncProcessing;
        this.batchSize = batchSize;
        this.updateTopkCache = updateTopkCache;
        this.recalculateVectors = recalculateVectors;
    }
    
    // Static factory methods
    public static SimilarityRecalculationRequest fullRecalculation() {
        return new SimilarityRecalculationRequest(null, null, "FULL", false, true, 100, true, true);
    }
    
    public static SimilarityRecalculationRequest forPlaces(List<Long> placeIds) {
        return new SimilarityRecalculationRequest(placeIds, null, "INCREMENTAL", false, true, 100, true, true);
    }
    
    public static SimilarityRecalculationRequest forUsers(List<Long> userIds) {
        return new SimilarityRecalculationRequest(null, userIds, "INCREMENTAL", false, true, 100, true, true);
    }
    
    // Getters and setters
    public List<Long> getPlaceIds() {
        return placeIds;
    }
    
    public void setPlaceIds(List<Long> placeIds) {
        this.placeIds = placeIds;
    }
    
    public List<Long> getUserIds() {
        return userIds;
    }
    
    public void setUserIds(List<Long> userIds) {
        this.userIds = userIds;
    }
    
    public String getRecalculationType() {
        return recalculationType;
    }
    
    public void setRecalculationType(String recalculationType) {
        this.recalculationType = recalculationType;
    }
    
    public Boolean getForceRecalculation() {
        return forceRecalculation;
    }
    
    public void setForceRecalculation(Boolean forceRecalculation) {
        this.forceRecalculation = forceRecalculation;
    }
    
    public Boolean getAsyncProcessing() {
        return asyncProcessing;
    }
    
    public void setAsyncProcessing(Boolean asyncProcessing) {
        this.asyncProcessing = asyncProcessing;
    }
    
    public Integer getBatchSize() {
        return batchSize;
    }
    
    public void setBatchSize(Integer batchSize) {
        this.batchSize = batchSize;
    }
    
    public Boolean getUpdateTopkCache() {
        return updateTopkCache;
    }
    
    public void setUpdateTopkCache(Boolean updateTopkCache) {
        this.updateTopkCache = updateTopkCache;
    }
    
    public Boolean getRecalculateVectors() {
        return recalculateVectors;
    }
    
    public void setRecalculateVectors(Boolean recalculateVectors) {
        this.recalculateVectors = recalculateVectors;
    }
}