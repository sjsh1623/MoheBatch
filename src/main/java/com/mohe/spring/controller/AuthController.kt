package com.mohe.spring.controller

import com.mohe.spring.dto.*
import com.mohe.spring.service.AuthService
import io.swagger.v3.oas.annotations.Operation
import io.swagger.v3.oas.annotations.Parameter
import io.swagger.v3.oas.annotations.media.Content
import io.swagger.v3.oas.annotations.media.ExampleObject
import io.swagger.v3.oas.annotations.media.Schema
import io.swagger.v3.oas.annotations.responses.ApiResponse as SwaggerApiResponse
import io.swagger.v3.oas.annotations.responses.ApiResponses
import io.swagger.v3.oas.annotations.security.SecurityRequirements
import io.swagger.v3.oas.annotations.tags.Tag
import jakarta.servlet.http.HttpServletRequest
import jakarta.validation.Valid
import org.springframework.http.HttpStatus
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.*

@RestController
@RequestMapping("/api/auth")
@Tag(name = "인증 관리", description = "사용자 인증, 회원가입, 로그인 관련 API")
class AuthController(
    private val authService: AuthService
) {
    
    @PostMapping("/login")
    @SecurityRequirements
    @Operation(
        summary = "사용자 로그인",
        description = "이메일과 비밀번호로 로그인하여 JWT 토큰을 발급받습니다."
    )
    @ApiResponses(
        value = [
            SwaggerApiResponse(
                responseCode = "200",
                description = "로그인 성공",
                content = [Content(
                    mediaType = "application/json",
                    schema = Schema(implementation = LoginResponse::class),
                    examples = [ExampleObject(
                        value = """
                        {
                          "success": true,
                          "data": {
                            "user": {
                              "id": "1",
                              "email": "user@example.com",
                              "nickname": "홍길동",
                              "isOnboardingCompleted": true,
                              "roles": ["ROLE_USER"]
                            },
                            "accessToken": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...",
                            "refreshToken": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...",
                            "tokenType": "Bearer",
                            "expiresIn": 3600
                          }
                        }
                        """
                    )]
                )]
            ),
            SwaggerApiResponse(
                responseCode = "400",
                description = "로그인 실패 (잘못된 인증 정보)",
                content = [Content(
                    mediaType = "application/json",
                    examples = [ExampleObject(
                        value = """
                        {
                          "success": false,
                          "error": {
                            "code": "INVALID_CREDENTIALS",
                            "message": "이메일 또는 비밀번호가 잘못되었습니다."
                          }
                        }
                        """
                    )]
                )]
            )
        ]
    )
    fun login(
        @Parameter(description = "로그인 요청 정보", required = true)
        @Valid @RequestBody request: LoginRequest,
        httpRequest: HttpServletRequest
    ): ResponseEntity<ApiResponse<LoginResponse>> {
        return try {
            val response = authService.login(request)
            ResponseEntity.ok(ApiResponse.success(response))
        } catch (e: Exception) {
            ResponseEntity.badRequest().body(
                ApiResponse.error(
                    code = ErrorCode.INVALID_CREDENTIALS,
                    message = e.message ?: "로그인에 실패했습니다",
                    path = httpRequest.requestURI
                )
            )
        }
    }
    
    @PostMapping("/signup")
    @SecurityRequirements
    @Operation(
        summary = "회원가입 시작",
        description = "이메일로 회원가입을 시작하고 인증 코드를 발송합니다."
    )
    @ApiResponses(
        value = [
            SwaggerApiResponse(
                responseCode = "200",
                description = "인증 코드 발송 성공",
                content = [Content(
                    mediaType = "application/json",
                    examples = [ExampleObject(
                        value = """
                        {
                          "success": true,
                          "message": "인증 코드가 이메일로 발송되었습니다.",
                          "data": {
                            "tempUserId": "temp_user_123"
                          }
                        }
                        """
                    )]
                )]
            ),
            SwaggerApiResponse(
                responseCode = "400",
                description = "이미 가입된 이메일",
                content = [Content(
                    mediaType = "application/json",
                    examples = [ExampleObject(
                        value = """
                        {
                          "success": false,
                          "error": {
                            "code": "DUPLICATE_EMAIL",
                            "message": "이미 사용 중인 이메일입니다"
                          }
                        }
                        """
                    )]
                )]
            )
        ]
    )
    fun signup(
        @Parameter(description = "회원가입 요청 정보", required = true)
        @Valid @RequestBody request: SignupRequest,
        httpRequest: HttpServletRequest
    ): ResponseEntity<ApiResponse<SignupResponse>> {
        return try {
            val response = authService.signup(request)
            ResponseEntity.ok(
                ApiResponse.success(
                    data = response,
                    message = "인증 코드가 이메일로 발송되었습니다."
                )
            )
        } catch (e: Exception) {
            val errorCode = when {
                e.message?.contains("이미 사용") == true -> ErrorCode.DUPLICATE_EMAIL
                else -> ErrorCode.VALIDATION_ERROR
            }
            ResponseEntity.badRequest().body(
                ApiResponse.error(
                    code = errorCode,
                    message = e.message ?: "회원가입에 실패했습니다",
                    path = httpRequest.requestURI
                )
            )
        }
    }
    
    @PostMapping("/verify-email")
    @SecurityRequirements
    @Operation(
        summary = "이메일 OTP 인증",
        description = "회원가입 과정에서 발송된 5자리 OTP 코드를 인증합니다."
    )
    @ApiResponses(
        value = [
            SwaggerApiResponse(
                responseCode = "200",
                description = "이메일 인증 성공",
                content = [Content(
                    mediaType = "application/json",
                    examples = [ExampleObject(
                        value = """
                        {
                          "success": true,
                          "message": "이메일 인증이 완료되었습니다.",
                          "data": {
                            "isVerified": true
                          }
                        }
                        """
                    )]
                )]
            )
        ]
    )
    fun verifyEmail(
        @Parameter(description = "이메일 인증 요청 정보", required = true)
        @Valid @RequestBody request: EmailVerificationRequest,
        httpRequest: HttpServletRequest
    ): ResponseEntity<ApiResponse<EmailVerificationResponse>> {
        return try {
            val response = authService.verifyEmail(request)
            ResponseEntity.ok(
                ApiResponse.success(
                    data = response,
                    message = "이메일 인증이 완료되었습니다."
                )
            )
        } catch (e: Exception) {
            val errorCode = when {
                e.message?.contains("인증 코드") == true -> ErrorCode.INVALID_VERIFICATION_CODE
                e.message?.contains("유효하지 않은") == true -> ErrorCode.VERIFICATION_CODE_EXPIRED
                else -> ErrorCode.VALIDATION_ERROR
            }
            ResponseEntity.badRequest().body(
                ApiResponse.error(
                    code = errorCode,
                    message = e.message ?: "이메일 인증에 실패했습니다",
                    path = httpRequest.requestURI
                )
            )
        }
    }
    
    @PostMapping("/check-nickname")
    @SecurityRequirements
    @Operation(
        summary = "닉네임 중복 확인",
        description = "회원가입 시 사용할 닉네임의 중복 여부를 확인합니다."
    )
    @ApiResponses(
        value = [
            SwaggerApiResponse(
                responseCode = "200",
                description = "닉네임 확인 완료",
                content = [Content(
                    mediaType = "application/json",
                    examples = [ExampleObject(
                        value = """
                        {
                          "success": true,
                          "data": {
                            "isAvailable": true,
                            "message": "사용 가능한 닉네임입니다."
                          }
                        }
                        """
                    )]
                )]
            )
        ]
    )
    fun checkNickname(
        @Parameter(description = "닉네임 중복 확인 요청", required = true)
        @Valid @RequestBody request: NicknameCheckRequest,
        httpRequest: HttpServletRequest
    ): ResponseEntity<ApiResponse<NicknameCheckResponse>> {
        return try {
            val response = authService.checkNickname(request)
            ResponseEntity.ok(ApiResponse.success(response))
        } catch (e: Exception) {
            ResponseEntity.badRequest().body(
                ApiResponse.error(
                    code = ErrorCode.VALIDATION_ERROR,
                    message = e.message ?: "닉네임 확인에 실패했습니다",
                    path = httpRequest.requestURI
                )
            )
        }
    }
    
    @PostMapping("/setup-password")
    @SecurityRequirements
    @Operation(
        summary = "비밀번호 설정 및 회원가입 완료",
        description = "이메일 인증 완료 후 닉네임과 비밀번호를 설정하여 회원가입을 완료합니다."
    )
    @ApiResponses(
        value = [
            SwaggerApiResponse(
                responseCode = "200",
                description = "회원가입 완료 및 자동 로그인",
                content = [Content(
                    mediaType = "application/json",
                    schema = Schema(implementation = LoginResponse::class)
                )]
            )
        ]
    )
    fun setupPassword(
        @Parameter(description = "비밀번호 설정 요청", required = true)
        @Valid @RequestBody request: PasswordSetupRequest,
        httpRequest: HttpServletRequest
    ): ResponseEntity<ApiResponse<LoginResponse>> {
        return try {
            val response = authService.setupPassword(request)
            ResponseEntity.ok(
                ApiResponse.success(
                    data = response,
                    message = "회원가입이 완료되었습니다."
                )
            )
        } catch (e: Exception) {
            val errorCode = when {
                e.message?.contains("닉네임") == true -> ErrorCode.DUPLICATE_NICKNAME
                e.message?.contains("약관") == true -> ErrorCode.VALIDATION_ERROR
                e.message?.contains("유효하지 않은") == true -> ErrorCode.VERIFICATION_CODE_EXPIRED
                else -> ErrorCode.VALIDATION_ERROR
            }
            ResponseEntity.badRequest().body(
                ApiResponse.error(
                    code = errorCode,
                    message = e.message ?: "회원가입에 실패했습니다",
                    path = httpRequest.requestURI
                )
            )
        }
    }
    
    @PostMapping("/refresh")
    @SecurityRequirements
    @Operation(
        summary = "토큰 갱신",
        description = "리프레시 토큰을 사용하여 새로운 액세스 토큰을 발급받습니다."
    )
    @ApiResponses(
        value = [
            SwaggerApiResponse(
                responseCode = "200",
                description = "토큰 갱신 성공",
                content = [Content(
                    mediaType = "application/json",
                    schema = Schema(implementation = TokenRefreshResponse::class)
                )]
            ),
            SwaggerApiResponse(
                responseCode = "401",
                description = "유효하지 않은 리프레시 토큰"
            )
        ]
    )
    fun refreshToken(
        @Parameter(description = "토큰 갱신 요청", required = true)
        @Valid @RequestBody request: TokenRefreshRequest,
        httpRequest: HttpServletRequest
    ): ResponseEntity<ApiResponse<TokenRefreshResponse>> {
        return try {
            val response = authService.refreshToken(request)
            ResponseEntity.ok(ApiResponse.success(response))
        } catch (e: Exception) {
            ResponseEntity.status(HttpStatus.UNAUTHORIZED).body(
                ApiResponse.error(
                    code = ErrorCode.INVALID_TOKEN,
                    message = e.message ?: "토큰 갱신에 실패했습니다",
                    path = httpRequest.requestURI
                )
            )
        }
    }
    
    @PostMapping("/logout")
    @SecurityRequirements
    @Operation(
        summary = "로그아웃",
        description = "리프레시 토큰을 무효화하여 로그아웃을 처리합니다."
    )
    @ApiResponses(
        value = [
            SwaggerApiResponse(
                responseCode = "200",
                description = "로그아웃 성공",
                content = [Content(
                    mediaType = "application/json",
                    examples = [ExampleObject(
                        value = """
                        {
                          "success": true,
                          "message": "로그아웃이 완료되었습니다."
                        }
                        """
                    )]
                )]
            )
        ]
    )
    fun logout(
        @Parameter(description = "로그아웃 요청", required = true)
        @Valid @RequestBody request: LogoutRequest,
        httpRequest: HttpServletRequest
    ): ResponseEntity<ApiResponse<Nothing>> {
        return try {
            authService.logout(request)
            ResponseEntity.ok(ApiResponse.success("로그아웃이 완료되었습니다."))
        } catch (e: Exception) {
            ResponseEntity.badRequest().body(
                ApiResponse.error(
                    code = ErrorCode.VALIDATION_ERROR,
                    message = e.message ?: "로그아웃에 실패했습니다",
                    path = httpRequest.requestURI
                )
            )
        }
    }
    
    @PostMapping("/forgot-password")
    @SecurityRequirements
    @Operation(
        summary = "비밀번호 재설정 요청",
        description = "등록된 이메일로 비밀번호 재설정 링크를 발송합니다."
    )
    @ApiResponses(
        value = [
            SwaggerApiResponse(
                responseCode = "200",
                description = "비밀번호 재설정 이메일 발송 성공",
                content = [Content(
                    mediaType = "application/json",
                    examples = [ExampleObject(
                        value = """
                        {
                          "success": true,
                          "message": "비밀번호 재설정 링크가 이메일로 발송되었습니다."
                        }
                        """
                    )]
                )]
            ),
            SwaggerApiResponse(
                responseCode = "400",
                description = "등록되지 않은 이메일"
            )
        ]
    )
    fun forgotPassword(
        @Parameter(description = "비밀번호 재설정 요청", required = true)
        @Valid @RequestBody request: ForgotPasswordRequest,
        httpRequest: HttpServletRequest
    ): ResponseEntity<ApiResponse<Nothing>> {
        return try {
            authService.forgotPassword(request)
            ResponseEntity.ok(ApiResponse.success("비밀번호 재설정 링크가 이메일로 발송되었습니다."))
        } catch (e: Exception) {
            ResponseEntity.badRequest().body(
                ApiResponse.error(
                    code = ErrorCode.RESOURCE_NOT_FOUND,
                    message = e.message ?: "비밀번호 재설정 요청에 실패했습니다",
                    path = httpRequest.requestURI
                )
            )
        }
    }
    
    @PostMapping("/reset-password")
    @SecurityRequirements
    @Operation(
        summary = "비밀번호 재설정 완료",
        description = "비밀번호 재설정 토큰을 사용하여 새로운 비밀번호로 변경합니다."
    )
    @ApiResponses(
        value = [
            SwaggerApiResponse(
                responseCode = "200",
                description = "비밀번호 재설정 성공",
                content = [Content(
                    mediaType = "application/json",
                    examples = [ExampleObject(
                        value = """
                        {
                          "success": true,
                          "message": "비밀번호가 성공적으로 변경되었습니다."
                        }
                        """
                    )]
                )]
            ),
            SwaggerApiResponse(
                responseCode = "400",
                description = "유효하지 않은 리셋 토큰"
            )
        ]
    )
    fun resetPassword(
        @Parameter(description = "비밀번호 재설정 요청", required = true)
        @Valid @RequestBody request: ResetPasswordRequest,
        httpRequest: HttpServletRequest
    ): ResponseEntity<ApiResponse<Nothing>> {
        return try {
            authService.resetPassword(request)
            ResponseEntity.ok(ApiResponse.success("비밀번호가 성공적으로 변경되었습니다."))
        } catch (e: Exception) {
            ResponseEntity.badRequest().body(
                ApiResponse.error(
                    code = ErrorCode.INVALID_TOKEN,
                    message = e.message ?: "비밀번호 재설정에 실패했습니다",
                    path = httpRequest.requestURI
                )
            )
        }
    }
}