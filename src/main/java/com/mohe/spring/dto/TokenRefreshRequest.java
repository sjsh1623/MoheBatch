package com.mohe.spring.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotBlank;

public class TokenRefreshRequest {
    
    @JsonProperty("refresh_token")
    @NotBlank(message = "리프레시 토큰은 필수입니다")
    private String refreshToken;
    
    // Default constructor
    public TokenRefreshRequest() {}
    
    // Constructor with fields
    public TokenRefreshRequest(String refreshToken) {
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