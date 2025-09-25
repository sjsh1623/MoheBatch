package com.mohe.spring.dto;

public class ProfileEditResponse {
    
    private ProfileEditData user;
    
    public ProfileEditResponse() {}
    
    public ProfileEditResponse(ProfileEditData user) {
        this.user = user;
    }
    
    public ProfileEditData getUser() {
        return user;
    }
    
    public void setUser(ProfileEditData user) {
        this.user = user;
    }
}