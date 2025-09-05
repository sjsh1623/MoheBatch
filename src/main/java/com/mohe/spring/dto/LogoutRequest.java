package com.mohe.spring.dto;

import com.fasterxml.jackson.annotation.JsonProperty;

public class LogoutRequest {
    
    @JsonProperty("refresh_token")
    private String refreshToken;
    
    // Default constructor
    public LogoutRequest() {}
    
    // Constructor with fields
    public LogoutRequest(String refreshToken) {
        this.refreshToken = refreshToken;
    }
    
    // Getters and setters
    public String getRefreshToken() {
        return refreshToken;
    }
    
    public void setRefreshToken(String refreshToken) {
        this.refreshToken = refreshToken;
    }
}