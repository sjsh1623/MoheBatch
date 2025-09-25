package com.mohe.spring.dto;

import java.util.List;

public class UserPreferencesData {
    
    private String mbti;
    private String ageRange;
    private List<String> spacePreferences;
    private String transportationMethod;
    
    public UserPreferencesData() {}
    
    public UserPreferencesData(String mbti, String ageRange, List<String> spacePreferences, String transportationMethod) {
        this.mbti = mbti;
        this.ageRange = ageRange;
        this.spacePreferences = spacePreferences;
        this.transportationMethod = transportationMethod;
    }
    
    public String getMbti() {
        return mbti;
    }
    
    public void setMbti(String mbti) {
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