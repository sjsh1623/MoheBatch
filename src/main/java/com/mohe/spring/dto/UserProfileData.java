package com.mohe.spring.dto;

import java.time.OffsetDateTime;
import java.util.List;

public class UserProfileData {
    
    private String id;
    private String email;
    private String nickname;
    private String mbti;
    private String ageRange;
    private List<String> spacePreferences;
    private String transportationMethod;
    private String profileImage;
    private OffsetDateTime createdAt;
    
    public UserProfileData() {}
    
    public UserProfileData(String id, String email, String nickname, String mbti, String ageRange, 
                          List<String> spacePreferences, String transportationMethod, String profileImage, 
                          OffsetDateTime createdAt) {
        this.id = id;
        this.email = email;
        this.nickname = nickname;
        this.mbti = mbti;
        this.ageRange = ageRange;
        this.spacePreferences = spacePreferences;
        this.transportationMethod = transportationMethod;
        this.profileImage = profileImage;
        this.createdAt = createdAt;
    }
    
    public String getId() {
        return id;
    }
    
    public void setId(String id) {
        this.id = id;
    }
    
    public String getEmail() {
        return email;
    }
    
    public void setEmail(String email) {
        this.email = email;
    }
    
    public String getNickname() {
        return nickname;
    }
    
    public void setNickname(String nickname) {
        this.nickname = nickname;
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
    
    public String getProfileImage() {
        return profileImage;
    }
    
    public void setProfileImage(String profileImage) {
        this.profileImage = profileImage;
    }
    
    public OffsetDateTime getCreatedAt() {
        return createdAt;
    }
    
    public void setCreatedAt(OffsetDateTime createdAt) {
        this.createdAt = createdAt;
    }
}