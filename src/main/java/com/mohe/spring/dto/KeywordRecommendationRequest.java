package com.mohe.spring.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.Size;
import java.util.List;

public class KeywordRecommendationRequest {
    
    @JsonProperty("keywords")
    @NotEmpty(message = "키워드는 최소 1개 이상 필요합니다")
    @Size(max = 10, message = "키워드는 최대 10개까지 가능합니다")
    private List<String> keywords;
    
    @JsonProperty("mbti")
    private String mbti;
    
    @JsonProperty("weight_boost")
    private Double weightBoost = 1.0;
    
    @JsonProperty("limit")
    private Integer limit = 20;
    
    // Default constructor
    public KeywordRecommendationRequest() {}
    
    // Constructor with fields
    public KeywordRecommendationRequest(List<String> keywords, String mbti, 
                                       Double weightBoost, Integer limit) {
        this.keywords = keywords;
        this.mbti = mbti;
        this.weightBoost = weightBoost;
        this.limit = limit;
    }
    
    // Getters and setters
    public List<String> getKeywords() {
        return keywords;
    }
    
    public void setKeywords(List<String> keywords) {
        this.keywords = keywords;
    }
    
    public String getMbti() {
        return mbti;
    }
    
    public void setMbti(String mbti) {
        this.mbti = mbti;
    }
    
    public Double getWeightBoost() {
        return weightBoost;
    }
    
    public void setWeightBoost(Double weightBoost) {
        this.weightBoost = weightBoost;
    }
    
    public Integer getLimit() {
        return limit;
    }
    
    public void setLimit(Integer limit) {
        this.limit = limit;
    }
}