package com.mohe.spring.dto;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.Email;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;
import java.util.List;

public class AuthDto {
    
    // Login Request/Response
    @Schema(description = "로그인 요청")
    public record LoginRequest(
        @NotBlank(message = "이메일 또는 ID는 필수입니다")
        @Schema(description = "사용자 이메일 또는 ID", example = "user@example.com or admin")
        String id,
        
        @NotBlank(message = "비밀번호는 필수입니다")
        @Schema(description = "사용자 비밀번호", example = "password123")
        String password
    ) {}

    @Schema(description = "로그인 응답")
    public record LoginResponse(
        @Schema(description = "사용자 정보")
        UserInfo user,
        @Schema(description = "액세스 토큰", example = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...")
        String accessToken,
        @Schema(description = "리프레시 토큰", example = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...")
        String refreshToken,
        @Schema(description = "토큰 타입", example = "Bearer")
        String tokenType,
        @Schema(description = "토큰 만료 시간 (초)", example = "3600")
        int expiresIn
    ) {
        public LoginResponse(UserInfo user, String accessToken, String refreshToken, int expiresIn) {
            this(user, accessToken, refreshToken, "Bearer", expiresIn);
        }
    }

    public record UserInfo(
        String id,
        String email,
        String nickname,
        boolean isOnboardingCompleted,
        List<String> roles
    ) {}

    // Signup Request/Response
    @Schema(description = "회원가입 요청")
    public record SignupRequest(
        @Email(message = "올바른 이메일 형식이 아닙니다")
        @NotBlank(message = "이메일은 필수입니다")
        @Schema(description = "가입할 이메일 주소", example = "user@example.com")
        String email
    ) {}

    public record SignupResponse(
        String tempUserId
    ) {}

    // Email Verification
    @Schema(description = "이메일 OTP 인증 요청")
    public record EmailVerificationRequest(
        @NotBlank(message = "임시 사용자 ID는 필수입니다")
        @Schema(description = "임시 사용자 ID", example = "temp_user_123")
        String tempUserId,
        
        @NotBlank(message = "인증 코드는 필수입니다")
        @Size(min = 5, max = 5, message = "인증 코드는 5자리입니다")
        @Schema(description = "5자리 OTP 인증 코드", example = "12345")
        String otpCode
    ) {}

    public record EmailVerificationResponse(
        boolean isVerified
    ) {}

    // Nickname Check
    public record NicknameCheckRequest(
        @NotBlank(message = "닉네임은 필수입니다")
        @Size(min = 2, max = 20, message = "닉네임은 2-20자 사이여야 합니다")
        String nickname
    ) {}

    public record NicknameCheckResponse(
        boolean isAvailable,
        String message
    ) {}

    // Password Setup
    public record PasswordSetupRequest(
        @NotBlank(message = "임시 사용자 ID는 필수입니다")
        String tempUserId,
        
        @NotBlank(message = "닉네임은 필수입니다")
        String nickname,
        
        @NotBlank(message = "비밀번호는 필수입니다")
        @Size(min = 8, max = 50, message = "비밀번호는 8-50자 사이여야 합니다")
        String password,
        
        boolean termsAgreed
    ) {
        public PasswordSetupRequest(String tempUserId, String nickname, String password) {
            this(tempUserId, nickname, password, false);
        }
    }

    // Token Refresh
    public record TokenRefreshRequest(
        @NotBlank(message = "리프레시 토큰은 필수입니다")
        String refreshToken
    ) {}

    public record TokenRefreshResponse(
        String accessToken,
        String tokenType,
        int expiresIn
    ) {
        public TokenRefreshResponse(String accessToken, int expiresIn) {
            this(accessToken, "Bearer", expiresIn);
        }
    }

    // Logout
    public record LogoutRequest(
        @NotBlank(message = "리프레시 토큰은 필수입니다")
        String refreshToken
    ) {}

    // Password Reset
    public record ForgotPasswordRequest(
        @Email(message = "올바른 이메일 형식이 아닙니다")
        @NotBlank(message = "이메일은 필수입니다")
        String email
    ) {}

    public record ResetPasswordRequest(
        @NotBlank(message = "리셋 토큰은 필수입니다")
        String token,
        
        @NotBlank(message = "새 비밀번호는 필수입니다")
        @Size(min = 8, max = 50, message = "비밀번호는 8-50자 사이여야 합니다")
        String newPassword
    ) {}
}