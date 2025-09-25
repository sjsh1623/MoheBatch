package com.mohe.spring.dto;

import java.util.List;

public class UserPreferencesRequest {
    
    private MbtiPreference mbti;
    private String ageRange;
    private List<String> spacePreferences;
    private String transportationMethod;
    
    public UserPreferencesRequest() {}
    
    public UserPreferencesRequest(MbtiPreference mbti, String ageRange, List<String> spacePreferences, String transportationMethod) {
        this.mbti = mbti;
        this.ageRange = ageRange;
        this.spacePreferences = spacePreferences;
        this.transportationMethod = transportationMethod;
    }
    
    public MbtiPreference getMbti() {
        return mbti;
    }
    
    public void setMbti(MbtiPreference mbti) {
        this.mbti = mbti;
    }
    
    public String getAgeRange() {
        return ageRange;
    }
    
    public void setAgeRange(String ageRange) {
        this.ageRange = ageRange;
    }
    
    public List<String> getSpacePreferences() {
        return spacePreferences;
    }
    
    public void setSpacePreferences(List<String> spacePreferences) {
        this.spacePreferences = spacePreferences;
    }
    
    public String getTransportationMethod() {
        return transportationMethod;
    }
    
    public void setTransportationMethod(String transportationMethod) {
        this.transportationMethod = transportationMethod;
    }
}