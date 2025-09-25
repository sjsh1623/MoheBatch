package com.mohe.spring.entity;

/**
 * Data class representing a selected keyword with confidence score
 * Used in keyword extraction results
 */
public class SelectedKeyword {
    
    private int keywordId;
    private String keyword;
    private double confidence;
    private String reasoning;
    
    // Default constructor
    public SelectedKeyword() {}
    
    // Constructor with required fields
    public SelectedKeyword(int keywordId, String keyword, double confidence) {
        this.keywordId = keywordId;
        this.keyword = keyword;
        this.confidence = confidence;
    }
    
    // Full constructor
    public SelectedKeyword(int keywordId, String keyword, double confidence, String reasoning) {
        this.keywordId = keywordId;
        this.keyword = keyword;
        this.confidence = confidence;
        this.reasoning = reasoning;
    }
    
    // Getters and Setters
    public int getKeywordId() {
        return keywordId;
    }
    
    public void setKeywordId(int keywordId) {
        this.keywordId = keywordId;
    }
    
    public String getKeyword() {
        return keyword;
    }
    
    public void setKeyword(String keyword) {
        this.keyword = keyword;
    }
    
    public double getConfidence() {
        return confidence;
    }
    
    public void setConfidence(double confidence) {
        this.confidence = confidence;
    }
    
    public String getReasoning() {
        return reasoning;
    }
    
    public void setReasoning(String reasoning) {
        this.reasoning = reasoning;
    }
    
    @Override
    public String toString() {
        return "SelectedKeyword{" +
                "keywordId=" + keywordId +
                ", keyword='" + keyword + '\'' +
                ", confidence=" + confidence +
                ", reasoning='" + reasoning + '\'' +
                '}';
    }
}