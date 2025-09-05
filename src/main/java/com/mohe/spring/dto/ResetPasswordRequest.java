package com.mohe.spring.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Size;

public class ResetPasswordRequest {
    
    @JsonProperty("token")
    @NotBlank(message = "토큰은 필수입니다")
    private String token;
    
    @JsonProperty("new_password")
    @NotBlank(message = "새 비밀번호는 필수입니다")
    @Size(min = 8, max = 50, message = "비밀번호는 8자 이상 50자 이하여야 합니다")
    @Pattern(regexp = "^(?=.*[a-z])(?=.*[A-Z])(?=.*\\d)(?=.*[@$!%*?&])[A-Za-z\\d@$!%*?&]",
             message = "비밀번호는 대문자, 소문자, 숫자, 특수문자를 모두 포함해야 합니다")
    private String newPassword;
    
    @JsonProperty("confirm_password")
    @NotBlank(message = "비밀번호 확인은 필수입니다")
    private String confirmPassword;
    
    // Default constructor
    public ResetPasswordRequest() {}
    
    // Constructor with fields
    public ResetPasswordRequest(String token, String newPassword, String confirmPassword) {
        this.token = token;
        this.newPassword = newPassword;
        this.confirmPassword = confirmPassword;
    }
    
    // Getters and setters
    public String getToken() {
        return token;
    }
    
    public void setToken(String token) {
        this.token = token;
    }
    
    public String getNewPassword() {
        return newPassword;
    }
    
    public void setNewPassword(String newPassword) {
        this.newPassword = newPassword;
    }
    
    public String getConfirmPassword() {
        return confirmPassword;
    }
    
    public void setConfirmPassword(String confirmPassword) {
        this.confirmPassword = confirmPassword;
    }
}