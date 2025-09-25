package com.mohe.spring.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import java.util.Map;

public class PlaceOptimizationResponse {
    
    @JsonProperty("job_id")
    private String jobId;
    
    @JsonProperty("status")
    private String status;
    
    @JsonProperty("processed_places")
    private Integer processedPlaces;
    
    @JsonProperty("total_places")
    private Integer totalPlaces;
    
    @JsonProperty("success_count")
    private Integer successCount;
    
    @JsonProperty("error_count")
    private Integer errorCount;
    
    @JsonProperty("processing_time_ms")
    private Long processingTimeMs;
    
    @JsonProperty("is_async")
    private Boolean isAsync;
    
    @JsonProperty("errors")
    private List<String> errors;
    
    @JsonProperty("results")
    private Map<Long, String> results;
    
    // Default constructor
    public PlaceOptimizationResponse() {}
    
    // Constructor with fields
    public PlaceOptimizationResponse(String jobId, String status, Integer processedPlaces,
                                    Integer totalPlaces, Integer successCount, Integer errorCount,
                                    Long processingTimeMs, Boolean isAsync, List<String> errors,
                                    Map<Long, String> results) {
        this.jobId = jobId;
        this.status = status;
        this.processedPlaces = processedPlaces;
        this.totalPlaces = totalPlaces;
        this.successCount = successCount;
        this.errorCount = errorCount;
        this.processingTimeMs = processingTimeMs;
        this.isAsync = isAsync;
        this.errors = errors;
        this.results = results;
    }
    
    // Static factory methods
    public static PlaceOptimizationResponse started(String jobId, Integer totalPlaces) {
        return new PlaceOptimizationResponse(jobId, "STARTED", 0, totalPlaces, 0, 0, 0L, true, null, null);
    }
    
    public static PlaceOptimizationResponse completed(String jobId, Integer successCount, Integer errorCount,
                                                     Long processingTime, Map<Long, String> results) {
        int total = successCount + errorCount;
        return new PlaceOptimizationResponse(jobId, "COMPLETED", total, total, successCount,
                                           errorCount, processingTime, false, null, results);
    }
    
    // Getters and setters
    public String getJobId() {
        return jobId;
    }
    
    public void setJobId(String jobId) {
        this.jobId = jobId;
    }
    
    public String getStatus() {
        return status;
    }
    
    public void setStatus(String status) {
        this.status = status;
    }
    
    public Integer getProcessedPlaces() {
        return processedPlaces;
    }
    
    public void setProcessedPlaces(Integer processedPlaces) {
        this.processedPlaces = processedPlaces;
    }
    
    public Integer getTotalPlaces() {
        return totalPlaces;
    }
    
    public void setTotalPlaces(Integer totalPlaces) {
        this.totalPlaces = totalPlaces;
    }
    
    public Integer getSuccessCount() {
        return successCount;
    }
    
    public void setSuccessCount(Integer successCount) {
        this.successCount = successCount;
    }
    
    public Integer getErrorCount() {
        return errorCount;
    }
    
    public void setErrorCount(Integer errorCount) {
        this.errorCount = errorCount;
    }
    
    public Long getProcessingTimeMs() {
        return processingTimeMs;
    }
    
    public void setProcessingTimeMs(Long processingTimeMs) {
        this.processingTimeMs = processingTimeMs;
    }
    
    public Boolean getIsAsync() {
        return isAsync;
    }
    
    public void setIsAsync(Boolean isAsync) {
        this.isAsync = isAsync;
    }
    
    public List<String> getErrors() {
        return errors;
    }
    
    public void setErrors(List<String> errors) {
        this.errors = errors;
    }
    
    public Map<Long, String> getResults() {
        return results;
    }
    
    public void setResults(Map<Long, String> results) {
        this.results = results;
    }
}