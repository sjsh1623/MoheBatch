package com.mohe.spring.dto;

import com.fasterxml.jackson.annotation.JsonProperty;

public class NicknameCheckResponse {
    
    @JsonProperty("available")
    private boolean available;
    
    @JsonProperty("message")
    private String message;
    
    // Default constructor
    public NicknameCheckResponse() {}
    
    // Constructor with fields
    public NicknameCheckResponse(boolean available, String message) {
        this.available = available;
        this.message = message;
    }
    
    // Static factory methods
    public static NicknameCheckResponse available(String message) {
        return new NicknameCheckResponse(true, message);
    }
    
    public static NicknameCheckResponse unavailable(String message) {
        return new NicknameCheckResponse(false, message);
    }
    
    // Getters and setters
    public boolean isAvailable() {
        return available;
    }
    
    public void setAvailable(boolean available) {
        this.available = available;
    }
    
    public String getMessage() {
        return message;
    }
    
    public void setMessage(String message) {
        this.message = message;
    }
}