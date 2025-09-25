package com.mohe.spring.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;

public class PasswordSetupRequest {
    
    @NotBlank(message = "임시 사용자 ID는 필수입니다")
    private String tempUserId;
    
    @NotBlank(message = "닉네임은 필수입니다")
    private String nickname;
    
    @NotBlank(message = "비밀번호는 필수입니다")
    @Size(min = 8, max = 50, message = "비밀번호는 8-50자 사이여야 합니다")
    private String password;
    
    private boolean termsAgreed;
    
    public PasswordSetupRequest() {}
    
    public PasswordSetupRequest(String tempUserId, String nickname, String password, boolean termsAgreed) {
        this.tempUserId = tempUserId;
        this.nickname = nickname;
        this.password = password;
        this.termsAgreed = termsAgreed;
    }
    
    public PasswordSetupRequest(String tempUserId, String nickname, String password) {
        this(tempUserId, nickname, password, false);
    }
    
    public String getTempUserId() {
        return tempUserId;
    }
    
    public void setTempUserId(String tempUserId) {
        this.tempUserId = tempUserId;
    }
    
    public String getNickname() {
        return nickname;
    }
    
    public void setNickname(String nickname) {
        this.nickname = nickname;
    }
    
    public String getPassword() {
        return password;
    }
    
    public void setPassword(String password) {
        this.password = password;
    }
    
    public boolean isTermsAgreed() {
        return termsAgreed;
    }
    
    public void setTermsAgreed(boolean termsAgreed) {
        this.termsAgreed = termsAgreed;
    }
}