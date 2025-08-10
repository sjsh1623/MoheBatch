package com.mohe.spring.service

import com.mohe.spring.dto.*
import com.mohe.spring.entity.*
import com.mohe.spring.repository.*
import com.mohe.spring.security.JwtTokenProvider
import org.springframework.security.authentication.AuthenticationManager
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken
import org.springframework.security.core.context.SecurityContextHolder
import org.springframework.security.crypto.password.PasswordEncoder
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional
import java.time.OffsetDateTime
import java.util.*
import kotlin.random.Random

@Service
@Transactional
class AuthService(
    private val userRepository: UserRepository,
    private val tempUserRepository: TempUserRepository,
    private val refreshTokenRepository: RefreshTokenRepository,
    private val passwordResetTokenRepository: PasswordResetTokenRepository,
    private val authenticationManager: AuthenticationManager,
    private val passwordEncoder: PasswordEncoder,
    private val jwtTokenProvider: JwtTokenProvider,
    private val emailService: EmailService
) {
    
    fun login(request: LoginRequest): LoginResponse {
        val authentication = authenticationManager.authenticate(
            UsernamePasswordAuthenticationToken(request.email, request.password)
        )
        
        SecurityContextHolder.getContext().authentication = authentication
        
        val user = userRepository.findByEmail(request.email)
            .orElseThrow { RuntimeException("사용자를 찾을 수 없습니다") }
        
        // Update last login time
        userRepository.updateLastLoginAt(user.id, OffsetDateTime.now())
        
        // Generate tokens
        val accessToken = jwtTokenProvider.generateAccessToken(authentication)
        val refreshToken = jwtTokenProvider.generateRefreshToken(user.id)
        
        // Save refresh token
        val refreshTokenEntity = RefreshToken(
            user = user,
            token = refreshToken,
            expiresAt = OffsetDateTime.now().plusSeconds(jwtTokenProvider.getRefreshTokenExpiration() / 1000)
        )
        refreshTokenRepository.save(refreshTokenEntity)
        
        return LoginResponse(
            user = UserInfo(
                id = user.id.toString(),
                email = user.email,
                nickname = user.nickname,
                isOnboardingCompleted = user.isOnboardingCompleted,
                roles = listOf("ROLE_USER")
            ),
            accessToken = accessToken,
            refreshToken = refreshToken,
            expiresIn = (jwtTokenProvider.getAccessTokenExpiration() / 1000).toInt()
        )
    }
    
    fun signup(request: SignupRequest): SignupResponse {
        if (userRepository.existsByEmail(request.email)) {
            throw RuntimeException("이미 사용 중인 이메일입니다")
        }
        
        // Delete existing temp user with same email
        tempUserRepository.findByEmail(request.email).ifPresent {
            tempUserRepository.delete(it)
        }
        
        val tempUserId = UUID.randomUUID().toString()
        val verificationCode = generateOTPCode()
        
        val tempUser = TempUser(
            id = tempUserId,
            email = request.email,
            verificationCode = verificationCode,
            expiresAt = OffsetDateTime.now().plusMinutes(10)
        )
        
        tempUserRepository.save(tempUser)
        
        // Send verification email
        emailService.sendVerificationEmail(request.email, verificationCode)
        
        return SignupResponse(tempUserId = tempUserId)
    }
    
    fun verifyEmail(request: EmailVerificationRequest): EmailVerificationResponse {
        val tempUser = tempUserRepository.findValidTempUser(request.tempUserId, OffsetDateTime.now())
            .orElseThrow { RuntimeException("유효하지 않은 인증 요청입니다") }
        
        if (tempUser.verificationCode != request.otpCode) {
            throw RuntimeException("인증 코드가 일치하지 않습니다")
        }
        
        return EmailVerificationResponse(isVerified = true)
    }
    
    fun checkNickname(request: NicknameCheckRequest): NicknameCheckResponse {
        val isAvailable = !userRepository.existsByNickname(request.nickname)
        
        return NicknameCheckResponse(
            isAvailable = isAvailable,
            message = if (isAvailable) "사용 가능한 닉네임입니다" else "이미 사용중인 닉네임입니다"
        )
    }
    
    fun setupPassword(request: PasswordSetupRequest): LoginResponse {
        val tempUser = tempUserRepository.findValidTempUser(request.tempUserId, OffsetDateTime.now())
            .orElseThrow { RuntimeException("유효하지 않은 회원가입 요청입니다") }
        
        if (!request.termsAgreed) {
            throw RuntimeException("약관 동의가 필요합니다")
        }
        
        if (userRepository.existsByNickname(request.nickname)) {
            throw RuntimeException("이미 사용중인 닉네임입니다")
        }
        
        val user = User(
            email = tempUser.email,
            passwordHash = passwordEncoder.encode(request.password),
            nickname = request.nickname,
            isOnboardingCompleted = false
        )
        
        val savedUser = userRepository.save(user)
        
        // Delete temp user
        tempUserRepository.delete(tempUser)
        
        // Generate tokens for immediate login
        val authentication = authenticationManager.authenticate(
            UsernamePasswordAuthenticationToken(user.email, request.password)
        )
        
        val accessToken = jwtTokenProvider.generateAccessToken(authentication)
        val refreshToken = jwtTokenProvider.generateRefreshToken(savedUser.id)
        
        // Save refresh token
        val refreshTokenEntity = RefreshToken(
            user = savedUser,
            token = refreshToken,
            expiresAt = OffsetDateTime.now().plusSeconds(jwtTokenProvider.getRefreshTokenExpiration() / 1000)
        )
        refreshTokenRepository.save(refreshTokenEntity)
        
        return LoginResponse(
            user = UserInfo(
                id = savedUser.id.toString(),
                email = savedUser.email,
                nickname = savedUser.nickname,
                isOnboardingCompleted = savedUser.isOnboardingCompleted,
                roles = listOf("ROLE_USER")
            ),
            accessToken = accessToken,
            refreshToken = refreshToken,
            expiresIn = (jwtTokenProvider.getAccessTokenExpiration() / 1000).toInt()
        )
    }
    
    fun refreshToken(request: TokenRefreshRequest): TokenRefreshResponse {
        val refreshToken = refreshTokenRepository.findValidToken(request.refreshToken, OffsetDateTime.now())
            .orElseThrow { RuntimeException("유효하지 않은 리프레시 토큰입니다") }
        
        val authentication = UsernamePasswordAuthenticationToken(
            refreshToken.user.email,
            null,
            listOf()
        )
        
        val accessToken = jwtTokenProvider.generateAccessToken(authentication)
        
        return TokenRefreshResponse(
            accessToken = accessToken,
            expiresIn = (jwtTokenProvider.getAccessTokenExpiration() / 1000).toInt()
        )
    }
    
    fun logout(request: LogoutRequest) {
        refreshTokenRepository.revokeByToken(request.refreshToken)
        SecurityContextHolder.clearContext()
    }
    
    fun forgotPassword(request: ForgotPasswordRequest) {
        val user = userRepository.findByEmail(request.email)
            .orElseThrow { RuntimeException("등록되지 않은 이메일입니다") }
        
        val resetToken = UUID.randomUUID().toString()
        val passwordResetToken = PasswordResetToken(
            user = user,
            token = resetToken,
            expiresAt = OffsetDateTime.now().plusHours(1)
        )
        
        passwordResetTokenRepository.save(passwordResetToken)
        
        // Send password reset email
        emailService.sendPasswordResetEmail(user.email, resetToken)
    }
    
    fun resetPassword(request: ResetPasswordRequest) {
        val resetToken = passwordResetTokenRepository.findValidToken(request.token, OffsetDateTime.now())
            .orElseThrow { RuntimeException("유효하지 않은 리셋 토큰입니다") }
        
        val user = resetToken.user
        val updatedUser = user.copy(passwordHash = passwordEncoder.encode(request.newPassword))
        userRepository.save(updatedUser)
        
        // Mark token as used
        passwordResetTokenRepository.markTokenAsUsed(request.token)
        
        // Revoke all refresh tokens for security
        refreshTokenRepository.revokeAllByUserId(user.id)
    }
    
    private fun generateOTPCode(): String {
        return String.format("%05d", Random.nextInt(0, 100000))
    }
}