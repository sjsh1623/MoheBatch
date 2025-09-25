package com.mohe.spring.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;

public class RecommendationExplanationResponse {
    
    @JsonProperty("place_id")
    private Long placeId;
    
    @JsonProperty("explanation")
    private String explanation;
    
    @JsonProperty("similarity_score")
    private double similarityScore;
    
    @JsonProperty("mbti_match")
    private boolean mbtiMatch;
    
    @JsonProperty("factors")
    private List<String> factors;
    
    @JsonProperty("confidence")
    private double confidence;
    
    // Default constructor
    public RecommendationExplanationResponse() {}
    
    // Constructor with fields
    public RecommendationExplanationResponse(Long placeId, String explanation, 
                                           double similarityScore, boolean mbtiMatch, 
                                           List<String> factors, double confidence) {
        this.placeId = placeId;
        this.explanation = explanation;
        this.similarityScore = similarityScore;
        this.mbtiMatch = mbtiMatch;
        this.factors = factors;
        this.confidence = confidence;
    }
    
    // Getters and setters
    public Long getPlaceId() {
        return placeId;
    }
    
    public void setPlaceId(Long placeId) {
        this.placeId = placeId;
    }
    
    public String getExplanation() {
        return explanation;
    }
    
    public void setExplanation(String explanation) {
        this.explanation = explanation;
    }
    
    public double getSimilarityScore() {
        return similarityScore;
    }
    
    public void setSimilarityScore(double similarityScore) {
        this.similarityScore = similarityScore;
    }
    
    public boolean isMbtiMatch() {
        return mbtiMatch;
    }
    
    public void setMbtiMatch(boolean mbtiMatch) {
        this.mbtiMatch = mbtiMatch;
    }
    
    public List<String> getFactors() {
        return factors;
    }
    
    public void setFactors(List<String> factors) {
        this.factors = factors;
    }
    
    public double getConfidence() {
        return confidence;
    }
    
    public void setConfidence(double confidence) {
        this.confidence = confidence;
    }
}