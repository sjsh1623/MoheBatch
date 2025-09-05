package com.mohe.spring.dto;

import io.swagger.v3.oas.annotations.media.Schema;

@Schema(description = "로그인 응답")
public class LoginResponse {
    
    @Schema(description = "사용자 정보")
    private UserInfo user;
    @Schema(description = "액세스 토큰", example = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...")
    private String accessToken;
    @Schema(description = "리프레시 토큰", example = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...")
    private String refreshToken;
    @Schema(description = "토큰 타입", example = "Bearer")
    private String tokenType;
    @Schema(description = "토큰 만료 시간 (초)", example = "3600")
    private int expiresIn;
    
    public LoginResponse() {}
    
    public LoginResponse(UserInfo user, String accessToken, String refreshToken, String tokenType, int expiresIn) {
        this.user = user;
        this.accessToken = accessToken;
        this.refreshToken = refreshToken;
        this.tokenType = tokenType;
        this.expiresIn = expiresIn;
    }
    
    public LoginResponse(UserInfo user, String accessToken, String refreshToken, int expiresIn) {
        this(user, accessToken, refreshToken, "Bearer", expiresIn);
    }
    
    public UserInfo getUser() {
        return user;
    }
    
    public void setUser(UserInfo user) {
        this.user = user;
    }
    
    public String getAccessToken() {
        return accessToken;
    }
    
    public void setAccessToken(String accessToken) {
        this.accessToken = accessToken;
    }
    
    public String getRefreshToken() {
        return refreshToken;
    }
    
    public void setRefreshToken(String refreshToken) {
        this.refreshToken = refreshToken;
    }
    
    public String getTokenType() {
        return tokenType;
    }
    
    public void setTokenType(String tokenType) {
        this.tokenType = tokenType;
    }
    
    public int getExpiresIn() {
        return expiresIn;
    }
    
    public void setExpiresIn(int expiresIn) {
        this.expiresIn = expiresIn;
    }
}