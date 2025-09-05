package com.mohe.spring.dto;

public class EmailVerificationResponse {
    
    private boolean isVerified;
    
    public EmailVerificationResponse() {}
    
    public EmailVerificationResponse(boolean isVerified) {
        this.isVerified = isVerified;
    }
    
    public boolean isVerified() {
        return isVerified;
    }
    
    public void setVerified(boolean verified) {
        isVerified = verified;
    }
}