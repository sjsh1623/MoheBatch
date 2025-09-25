package com.mohe.spring.service;

import java.time.LocalDateTime;
import java.util.Map;

public class SimilarityStatistics {
    private long totalPlaces;
    private long totalSimilarities;
    private long totalTopKEntries;
    private LocalDateTime lastCalculationTime;
    private Map<String, Object> additionalStats;
    private double averageJaccard;
    private double averageCosine;
    private long sampleSize;
    
    public SimilarityStatistics() {}
    
    public SimilarityStatistics(long totalPlaces, long totalSimilarities, long totalTopKEntries, 
                               LocalDateTime lastCalculationTime, Map<String, Object> additionalStats) {
        this.totalPlaces = totalPlaces;
        this.totalSimilarities = totalSimilarities;
        this.totalTopKEntries = totalTopKEntries;
        this.lastCalculationTime = lastCalculationTime;
        this.additionalStats = additionalStats;
    }
    
    public long getTotalPlaces() {
        return totalPlaces;
    }
    
    public void setTotalPlaces(long totalPlaces) {
        this.totalPlaces = totalPlaces;
    }
    
    public long getTotalSimilarities() {
        return totalSimilarities;
    }
    
    public void setTotalSimilarities(long totalSimilarities) {
        this.totalSimilarities = totalSimilarities;
    }
    
    public long getTotalTopKEntries() {
        return totalTopKEntries;
    }
    
    public void setTotalTopKEntries(long totalTopKEntries) {
        this.totalTopKEntries = totalTopKEntries;
    }
    
    public LocalDateTime getLastCalculationTime() {
        return lastCalculationTime;
    }
    
    public void setLastCalculationTime(LocalDateTime lastCalculationTime) {
        this.lastCalculationTime = lastCalculationTime;
    }
    
    public Map<String, Object> getAdditionalStats() {
        return additionalStats;
    }
    
    public void setAdditionalStats(Map<String, Object> additionalStats) {
        this.additionalStats = additionalStats;
    }
    
    public double getAverageJaccard() {
        return averageJaccard;
    }
    
    public void setAverageJaccard(double averageJaccard) {
        this.averageJaccard = averageJaccard;
    }
    
    public double getAverageCosine() {
        return averageCosine;
    }
    
    public void setAverageCosine(double averageCosine) {
        this.averageCosine = averageCosine;
    }
    
    public long getSampleSize() {
        return sampleSize;
    }
    
    public void setSampleSize(long sampleSize) {
        this.sampleSize = sampleSize;
    }
}