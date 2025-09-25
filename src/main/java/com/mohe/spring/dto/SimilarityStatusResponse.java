package com.mohe.spring.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.time.LocalDateTime;
import java.util.Map;

public class SimilarityStatusResponse {
    
    @JsonProperty("matrix_status")
    private String matrixStatus;
    
    @JsonProperty("last_calculation")
    private LocalDateTime lastCalculation;
    
    @JsonProperty("next_scheduled_calculation")
    private LocalDateTime nextScheduledCalculation;
    
    @JsonProperty("total_similarities")
    private Long totalSimilarities;
    
    @JsonProperty("topk_cache_size")
    private Long topkCacheSize;
    
    @JsonProperty("vector_similarities")
    private Long vectorSimilarities;
    
    @JsonProperty("calculation_in_progress")
    private Boolean calculationInProgress;
    
    @JsonProperty("last_calculation_time_ms")
    private Long lastCalculationTimeMs;
    
    @JsonProperty("statistics")
    private SimilarityStatistics statistics;
    
    // Default constructor
    public SimilarityStatusResponse() {}
    
    // Constructor with fields
    public SimilarityStatusResponse(String matrixStatus, LocalDateTime lastCalculation,
                                   LocalDateTime nextScheduledCalculation, Long totalSimilarities,
                                   Long topkCacheSize, Long vectorSimilarities,
                                   Boolean calculationInProgress, Long lastCalculationTimeMs,
                                   SimilarityStatistics statistics) {
        this.matrixStatus = matrixStatus;
        this.lastCalculation = lastCalculation;
        this.nextScheduledCalculation = nextScheduledCalculation;
        this.totalSimilarities = totalSimilarities;
        this.topkCacheSize = topkCacheSize;
        this.vectorSimilarities = vectorSimilarities;
        this.calculationInProgress = calculationInProgress;
        this.lastCalculationTimeMs = lastCalculationTimeMs;
        this.statistics = statistics;
    }
    
    // Nested class for statistics
    public static class SimilarityStatistics {
        @JsonProperty("average_jaccard")
        private Double averageJaccard;
        
        @JsonProperty("average_cosine")
        private Double averageCosine;
        
        @JsonProperty("mbti_weighted_count")
        private Long mbtiWeightedCount;
        
        @JsonProperty("active_users")
        private Long activeUsers;
        
        @JsonProperty("active_places")
        private Long activePlaces;
        
        @JsonProperty("performance_metrics")
        private Map<String, Object> performanceMetrics;
        
        // Default constructor
        public SimilarityStatistics() {}
        
        // Constructor
        public SimilarityStatistics(Double averageJaccard, Double averageCosine,
                                   Long mbtiWeightedCount, Long activeUsers,
                                   Long activePlaces, Map<String, Object> performanceMetrics) {
            this.averageJaccard = averageJaccard;
            this.averageCosine = averageCosine;
            this.mbtiWeightedCount = mbtiWeightedCount;
            this.activeUsers = activeUsers;
            this.activePlaces = activePlaces;
            this.performanceMetrics = performanceMetrics;
        }
        
        // Getters and setters
        public Double getAverageJaccard() { return averageJaccard; }
        public void setAverageJaccard(Double averageJaccard) { this.averageJaccard = averageJaccard; }
        public Double getAverageCosine() { return averageCosine; }
        public void setAverageCosine(Double averageCosine) { this.averageCosine = averageCosine; }
        public Long getMbtiWeightedCount() { return mbtiWeightedCount; }
        public void setMbtiWeightedCount(Long mbtiWeightedCount) { this.mbtiWeightedCount = mbtiWeightedCount; }
        public Long getActiveUsers() { return activeUsers; }
        public void setActiveUsers(Long activeUsers) { this.activeUsers = activeUsers; }
        public Long getActivePlaces() { return activePlaces; }
        public void setActivePlaces(Long activePlaces) { this.activePlaces = activePlaces; }
        public Map<String, Object> getPerformanceMetrics() { return performanceMetrics; }
        public void setPerformanceMetrics(Map<String, Object> performanceMetrics) { this.performanceMetrics = performanceMetrics; }
    }
    
    // Getters and setters
    public String getMatrixStatus() {
        return matrixStatus;
    }
    
    public void setMatrixStatus(String matrixStatus) {
        this.matrixStatus = matrixStatus;
    }
    
    public LocalDateTime getLastCalculation() {
        return lastCalculation;
    }
    
    public void setLastCalculation(LocalDateTime lastCalculation) {
        this.lastCalculation = lastCalculation;
    }
    
    public LocalDateTime getNextScheduledCalculation() {
        return nextScheduledCalculation;
    }
    
    public void setNextScheduledCalculation(LocalDateTime nextScheduledCalculation) {
        this.nextScheduledCalculation = nextScheduledCalculation;
    }
    
    public Long getTotalSimilarities() {
        return totalSimilarities;
    }
    
    public void setTotalSimilarities(Long totalSimilarities) {
        this.totalSimilarities = totalSimilarities;
    }
    
    public Long getTopkCacheSize() {
        return topkCacheSize;
    }
    
    public void setTopkCacheSize(Long topkCacheSize) {
        this.topkCacheSize = topkCacheSize;
    }
    
    public Long getVectorSimilarities() {
        return vectorSimilarities;
    }
    
    public void setVectorSimilarities(Long vectorSimilarities) {
        this.vectorSimilarities = vectorSimilarities;
    }
    
    public Boolean getCalculationInProgress() {
        return calculationInProgress;
    }
    
    public void setCalculationInProgress(Boolean calculationInProgress) {
        this.calculationInProgress = calculationInProgress;
    }
    
    public Long getLastCalculationTimeMs() {
        return lastCalculationTimeMs;
    }
    
    public void setLastCalculationTimeMs(Long lastCalculationTimeMs) {
        this.lastCalculationTimeMs = lastCalculationTimeMs;
    }
    
    public SimilarityStatistics getStatistics() {
        return statistics;
    }
    
    public void setStatistics(SimilarityStatistics statistics) {
        this.statistics = statistics;
    }
}