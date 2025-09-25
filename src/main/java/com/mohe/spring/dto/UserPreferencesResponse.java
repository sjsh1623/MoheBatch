package com.mohe.spring.dto;

public class UserPreferencesResponse {
    
    private UserPreferencesData preferences;
    
    public UserPreferencesResponse() {}
    
    public UserPreferencesResponse(UserPreferencesData preferences) {
        this.preferences = preferences;
    }
    
    public UserPreferencesData getPreferences() {
        return preferences;
    }
    
    public void setPreferences(UserPreferencesData preferences) {
        this.preferences = preferences;
    }
}