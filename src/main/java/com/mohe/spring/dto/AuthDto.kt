package com.mohe.spring.dto

import io.swagger.v3.oas.annotations.media.Schema
import jakarta.validation.constraints.Email
import jakarta.validation.constraints.NotBlank
import jakarta.validation.constraints.Size

// Login Request/Response
@Schema(description = "로그인 요청")
data class LoginRequest(
    @field:Email(message = "올바른 이메일 형식이 아닙니다")
    @field:NotBlank(message = "이메일은 필수입니다")
    @Schema(description = "사용자 이메일", example = "user@example.com")
    val email: String,
    
    @field:NotBlank(message = "비밀번호는 필수입니다")
    @Schema(description = "사용자 비밀번호", example = "password123")
    val password: String
)

@Schema(description = "로그인 응답")
data class LoginResponse(
    @Schema(description = "사용자 정보")
    val user: UserInfo,
    @Schema(description = "액세스 토큰", example = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...")
    val accessToken: String,
    @Schema(description = "리프레시 토큰", example = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...")
    val refreshToken: String,
    @Schema(description = "토큰 타입", example = "Bearer")
    val tokenType: String = "Bearer",
    @Schema(description = "토큰 만료 시간 (초)", example = "3600")
    val expiresIn: Int
)

data class UserInfo(
    val id: String,
    val email: String,
    val nickname: String?,
    val isOnboardingCompleted: Boolean,
    val roles: List<String>
)

// Signup Request/Response
@Schema(description = "회원가입 요청")
data class SignupRequest(
    @field:Email(message = "올바른 이메일 형식이 아닙니다")
    @field:NotBlank(message = "이메일은 필수입니다")
    @Schema(description = "가입할 이메일 주소", example = "user@example.com")
    val email: String
)

data class SignupResponse(
    val tempUserId: String
)

// Email Verification
@Schema(description = "이메일 OTP 인증 요청")
data class EmailVerificationRequest(
    @field:NotBlank(message = "임시 사용자 ID는 필수입니다")
    @Schema(description = "임시 사용자 ID", example = "temp_user_123")
    val tempUserId: String,
    
    @field:NotBlank(message = "인증 코드는 필수입니다")
    @field:Size(min = 5, max = 5, message = "인증 코드는 5자리입니다")
    @Schema(description = "5자리 OTP 인증 코드", example = "12345")
    val otpCode: String
)

data class EmailVerificationResponse(
    val isVerified: Boolean
)

// Nickname Check
data class NicknameCheckRequest(
    @field:NotBlank(message = "닉네임은 필수입니다")
    @field:Size(min = 2, max = 20, message = "닉네임은 2-20자 사이여야 합니다")
    val nickname: String
)

data class NicknameCheckResponse(
    val isAvailable: Boolean,
    val message: String
)

// Password Setup
data class PasswordSetupRequest(
    @field:NotBlank(message = "임시 사용자 ID는 필수입니다")
    val tempUserId: String,
    
    @field:NotBlank(message = "닉네임은 필수입니다")
    val nickname: String,
    
    @field:NotBlank(message = "비밀번호는 필수입니다")
    @field:Size(min = 8, max = 50, message = "비밀번호는 8-50자 사이여야 합니다")
    val password: String,
    
    val termsAgreed: Boolean = false
)

// Token Refresh
data class TokenRefreshRequest(
    @field:NotBlank(message = "리프레시 토큰은 필수입니다")
    val refreshToken: String
)

data class TokenRefreshResponse(
    val accessToken: String,
    val tokenType: String = "Bearer",
    val expiresIn: Int
)

// Logout
data class LogoutRequest(
    @field:NotBlank(message = "리프레시 토큰은 필수입니다")
    val refreshToken: String
)

// Password Reset
data class ForgotPasswordRequest(
    @field:Email(message = "올바른 이메일 형식이 아닙니다")
    @field:NotBlank(message = "이메일은 필수입니다")
    val email: String
)

data class ResetPasswordRequest(
    @field:NotBlank(message = "리셋 토큰은 필수입니다")
    val token: String,
    
    @field:NotBlank(message = "새 비밀번호는 필수입니다")
    @field:Size(min = 8, max = 50, message = "비밀번호는 8-50자 사이여야 합니다")
    val newPassword: String
)