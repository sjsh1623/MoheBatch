package com.mohe.spring.dto;

import java.util.List;

public class UserInfo {
    
    private String id;
    private String email;
    private String nickname;
    private boolean isOnboardingCompleted;
    private List<String> roles;
    
    public UserInfo() {}
    
    public UserInfo(String id, String email, String nickname, boolean isOnboardingCompleted, List<String> roles) {
        this.id = id;
        this.email = email;
        this.nickname = nickname;
        this.isOnboardingCompleted = isOnboardingCompleted;
        this.roles = roles;
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
    
    public boolean isOnboardingCompleted() {
        return isOnboardingCompleted;
    }
    
    public void setOnboardingCompleted(boolean onboardingCompleted) {
        isOnboardingCompleted = onboardingCompleted;
    }
    
    public List<String> getRoles() {
        return roles;
    }
    
    public void setRoles(List<String> roles) {
        this.roles = roles;
    }
}