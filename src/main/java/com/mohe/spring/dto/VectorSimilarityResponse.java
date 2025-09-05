package com.mohe.spring.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;

public class VectorSimilarityResponse {
    
    @JsonProperty("similar_places")
    private List<SimilarPlace> similarPlaces;
    
    @JsonProperty("user_id")
    private Long userId;
    
    @JsonProperty("query_place_id")
    private Long queryPlaceId;
    
    @JsonProperty("total_matches")
    private Integer totalMatches;
    
    @JsonProperty("similarity_threshold")
    private Double similarityThreshold;
    
    @JsonProperty("vector_type")
    private String vectorType;
    
    @JsonProperty("search_time_ms")
    private Long searchTimeMs;
    
    // Default constructor
    public VectorSimilarityResponse() {}
    
    // Constructor with fields
    public VectorSimilarityResponse(List<SimilarPlace> similarPlaces, Long userId,
                                   Long queryPlaceId, Integer totalMatches,
                                   Double similarityThreshold, String vectorType,
                                   Long searchTimeMs) {
        this.similarPlaces = similarPlaces;
        this.userId = userId;
        this.queryPlaceId = queryPlaceId;
        this.totalMatches = totalMatches;
        this.similarityThreshold = similarityThreshold;
        this.vectorType = vectorType;
        this.searchTimeMs = searchTimeMs;
    }
    
    // Static factory method
    public static VectorSimilarityResponse of(List<SimilarPlace> similarPlaces, Long userId,
                                             Long queryPlaceId, Double similarityThreshold,
                                             String vectorType, Long searchTimeMs) {
        return new VectorSimilarityResponse(similarPlaces, userId, queryPlaceId,
                                          similarPlaces != null ? similarPlaces.size() : 0,
                                          similarityThreshold, vectorType, searchTimeMs);
    }
    
    // Nested class for similar place
    public static class SimilarPlace {
        @JsonProperty("place")
        private PlaceDto place;
        
        @JsonProperty("similarity_score")
        private Double similarityScore;
        
        @JsonProperty("vector_similarity")
        private Double vectorSimilarity;
        
        @JsonProperty("weighted_similarity")
        private Double weightedSimilarity;
        
        @JsonProperty("explanation")
        private String explanation;
        
        // Default constructor
        public SimilarPlace() {}
        
        // Constructor
        public SimilarPlace(PlaceDto place, Double similarityScore, Double vectorSimilarity,
                           Double weightedSimilarity, String explanation) {
            this.place = place;
            this.similarityScore = similarityScore;
            this.vectorSimilarity = vectorSimilarity;
            this.weightedSimilarity = weightedSimilarity;
            this.explanation = explanation;
        }
        
        // Getters and setters
        public PlaceDto getPlace() { return place; }
        public void setPlace(PlaceDto place) { this.place = place; }
        public Double getSimilarityScore() { return similarityScore; }
        public void setSimilarityScore(Double similarityScore) { this.similarityScore = similarityScore; }
        public Double getVectorSimilarity() { return vectorSimilarity; }
        public void setVectorSimilarity(Double vectorSimilarity) { this.vectorSimilarity = vectorSimilarity; }
        public Double getWeightedSimilarity() { return weightedSimilarity; }
        public void setWeightedSimilarity(Double weightedSimilarity) { this.weightedSimilarity = weightedSimilarity; }
        public String getExplanation() { return explanation; }
        public void setExplanation(String explanation) { this.explanation = explanation; }
    }
    
    // Getters and setters
    public List<SimilarPlace> getSimilarPlaces() {
        return similarPlaces;
    }
    
    public void setSimilarPlaces(List<SimilarPlace> similarPlaces) {
        this.similarPlaces = similarPlaces;
    }
    
    public Long getUserId() {
        return userId;
    }
    
    public void setUserId(Long userId) {
        this.userId = userId;
    }
    
    public Long getQueryPlaceId() {
        return queryPlaceId;
    }
    
    public void setQueryPlaceId(Long queryPlaceId) {
        this.queryPlaceId = queryPlaceId;
    }
    
    public Integer getTotalMatches() {
        return totalMatches;
    }
    
    public void setTotalMatches(Integer totalMatches) {
        this.totalMatches = totalMatches;
    }
    
    public Double getSimilarityThreshold() {
        return similarityThreshold;
    }
    
    public void setSimilarityThreshold(Double similarityThreshold) {
        this.similarityThreshold = similarityThreshold;
    }
    
    public String getVectorType() {
        return vectorType;
    }
    
    public void setVectorType(String vectorType) {
        this.vectorType = vectorType;
    }
    
    public Long getSearchTimeMs() {
        return searchTimeMs;
    }
    
    public void setSearchTimeMs(Long searchTimeMs) {
        this.searchTimeMs = searchTimeMs;
    }
}