package com.mohe.spring.dto;

public class UserProfileResponse {
    
    private UserProfileData user;
    
    public UserProfileResponse() {}
    
    public UserProfileResponse(UserProfileData user) {
        this.user = user;
    }
    
    public UserProfileData getUser() {
        return user;
    }
    
    public void setUser(UserProfileData user) {
        this.user = user;
    }
}