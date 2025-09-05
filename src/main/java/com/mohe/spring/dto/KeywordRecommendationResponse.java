package com.mohe.spring.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;

public class KeywordRecommendationResponse {
    
    @JsonProperty("places")
    private List<PlaceDto> places;
    
    @JsonProperty("matched_keywords")
    private List<String> matchedKeywords;
    
    @JsonProperty("keyword_coverage")
    private Double keywordCoverage;
    
    @JsonProperty("mbti_weighted")
    private Boolean mbtiWeighted;
    
    @JsonProperty("total_count")
    private int totalCount;
    
    @JsonProperty("search_time_ms")
    private Long searchTimeMs;
    
    // Default constructor
    public KeywordRecommendationResponse() {}
    
    // Constructor with fields
    public KeywordRecommendationResponse(List<PlaceDto> places, List<String> matchedKeywords,
                                        Double keywordCoverage, Boolean mbtiWeighted,
                                        int totalCount, Long searchTimeMs) {
        this.places = places;
        this.matchedKeywords = matchedKeywords;
        this.keywordCoverage = keywordCoverage;
        this.mbtiWeighted = mbtiWeighted;
        this.totalCount = totalCount;
        this.searchTimeMs = searchTimeMs;
    }
    
    // Static factory method
    public static KeywordRecommendationResponse of(List<PlaceDto> places, List<String> matchedKeywords,
                                                  Double keywordCoverage, Boolean mbtiWeighted,
                                                  Long searchTimeMs) {
        return new KeywordRecommendationResponse(places, matchedKeywords, keywordCoverage,
                                                mbtiWeighted, places != null ? places.size() : 0,
                                                searchTimeMs);
    }
    
    // Getters and setters
    public List<PlaceDto> getPlaces() {
        return places;
    }
    
    public void setPlaces(List<PlaceDto> places) {
        this.places = places;
    }
    
    public List<String> getMatchedKeywords() {
        return matchedKeywords;
    }
    
    public void setMatchedKeywords(List<String> matchedKeywords) {
        this.matchedKeywords = matchedKeywords;
    }
    
    public Double getKeywordCoverage() {
        return keywordCoverage;
    }
    
    public void setKeywordCoverage(Double keywordCoverage) {
        this.keywordCoverage = keywordCoverage;
    }
    
    public Boolean getMbtiWeighted() {
        return mbtiWeighted;
    }
    
    public void setMbtiWeighted(Boolean mbtiWeighted) {
        this.mbtiWeighted = mbtiWeighted;
    }
    
    public int getTotalCount() {
        return totalCount;
    }
    
    public void setTotalCount(int totalCount) {
        this.totalCount = totalCount;
    }
    
    public Long getSearchTimeMs() {
        return searchTimeMs;
    }
    
    public void setSearchTimeMs(Long searchTimeMs) {
        this.searchTimeMs = searchTimeMs;
    }
}