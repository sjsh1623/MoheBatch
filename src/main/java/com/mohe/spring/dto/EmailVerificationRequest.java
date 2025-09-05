package com.mohe.spring.dto;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;

@Schema(description = "이메일 OTP 인증 요청")
public class EmailVerificationRequest {
    
    @NotBlank(message = "임시 사용자 ID는 필수입니다")
    @Schema(description = "임시 사용자 ID", example = "temp_user_123")
    private String tempUserId;
    
    @NotBlank(message = "인증 코드는 필수입니다")
    @Size(min = 5, max = 5, message = "인증 코드는 5자리입니다")
    @Schema(description = "5자리 OTP 인증 코드", example = "12345")
    private String otpCode;
    
    public EmailVerificationRequest() {}
    
    public EmailVerificationRequest(String tempUserId, String otpCode) {
        this.tempUserId = tempUserId;
        this.otpCode = otpCode;
    }
    
    public String getTempUserId() {
        return tempUserId;
    }
    
    public void setTempUserId(String tempUserId) {
        this.tempUserId = tempUserId;
    }
    
    public String getOtpCode() {
        return otpCode;
    }
    
    public void setOtpCode(String otpCode) {
        this.otpCode = otpCode;
    }
}