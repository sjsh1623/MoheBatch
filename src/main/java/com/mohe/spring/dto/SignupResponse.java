package com.mohe.spring.dto;

public class SignupResponse {
    
    private String tempUserId;
    
    public SignupResponse() {}
    
    public SignupResponse(String tempUserId) {
        this.tempUserId = tempUserId;
    }
    
    public String getTempUserId() {
        return tempUserId;
    }
    
    public void setTempUserId(String tempUserId) {
        this.tempUserId = tempUserId;
    }
}