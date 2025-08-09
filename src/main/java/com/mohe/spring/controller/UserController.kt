package com.mohe.spring.controller

import com.mohe.spring.dto.*
import com.mohe.spring.service.UserService
import io.swagger.v3.oas.annotations.Operation
import io.swagger.v3.oas.annotations.Parameter
import io.swagger.v3.oas.annotations.media.Content
import io.swagger.v3.oas.annotations.media.ExampleObject
import io.swagger.v3.oas.annotations.media.Schema
import io.swagger.v3.oas.annotations.responses.ApiResponse as SwaggerApiResponse
import io.swagger.v3.oas.annotations.responses.ApiResponses
import io.swagger.v3.oas.annotations.security.SecurityRequirement
import io.swagger.v3.oas.annotations.tags.Tag
import jakarta.servlet.http.HttpServletRequest
import jakarta.validation.Valid
import org.springframework.http.ResponseEntity
import org.springframework.security.access.prepost.PreAuthorize
import org.springframework.web.bind.annotation.*

@RestController
@RequestMapping("/api/user")
@PreAuthorize("hasRole('USER')")
@SecurityRequirement(name = "bearerAuth")
@Tag(name = "사용자 관리", description = "사용자 프로필 및 선호도 관리 API")
class UserController(
    private val userService: UserService
) {
    
    @PutMapping("/preferences")
    @Operation(
        summary = "사용자 선호도 설정",
        description = "MBTI, 연령대, 공간 선호도, 교통수단 등 사용자 선호도를 설정합니다."
    )
    @ApiResponses(
        value = [
            SwaggerApiResponse(
                responseCode = "200",
                description = "선호도 설정 성공",
                content = [Content(
                    mediaType = "application/json",
                    schema = Schema(implementation = UserPreferencesResponse::class),
                    examples = [ExampleObject(
                        value = """
                        {
                          "success": true,
                          "message": "선호도 설정이 완료되었습니다.",
                          "data": {
                            "preferences": {
                              "mbti": "ENTJ",
                              "ageRange": "20",
                              "spacePreferences": ["workshop", "exhibition", "nature"],
                              "transportationMethod": "public"
                            }
                          }
                        }
                        """
                    )]
                )]
            )
        ]
    )
    fun updatePreferences(
        @Parameter(description = "사용자 선호도 설정 요청", required = true)
        @Valid @RequestBody request: UserPreferencesRequest,
        httpRequest: HttpServletRequest
    ): ResponseEntity<ApiResponse<UserPreferencesResponse>> {
        return try {
            val response = userService.updatePreferences(request)
            ResponseEntity.ok(
                ApiResponse.success(
                    data = response,
                    message = "선호도 설정이 완료되었습니다."
                )
            )
        } catch (e: Exception) {
            ResponseEntity.badRequest().body(
                ApiResponse.error(
                    code = ErrorCode.VALIDATION_ERROR,
                    message = e.message ?: "선호도 설정에 실패했습니다",
                    path = httpRequest.requestURI
                )
            )
        }
    }
    
    @GetMapping("/profile")
    @Operation(
        summary = "사용자 프로필 조회",
        description = "현재 로그인된 사용자의 프로필 정보를 조회합니다."
    )
    @ApiResponses(
        value = [
            SwaggerApiResponse(
                responseCode = "200",
                description = "프로필 조회 성공",
                content = [Content(
                    mediaType = "application/json",
                    schema = Schema(implementation = UserProfileResponse::class),
                    examples = [ExampleObject(
                        value = """
                        {
                          "success": true,
                          "data": {
                            "user": {
                              "id": "1",
                              "email": "user@example.com",
                              "nickname": "홍길동",
                              "mbti": "ENTJ",
                              "ageRange": "20",
                              "spacePreferences": ["workshop", "exhibition", "nature"],
                              "transportationMethod": "public",
                              "profileImage": "https://example.com/profile.jpg",
                              "createdAt": "2024-01-01T00:00:00Z"
                            }
                          }
                        }
                        """
                    )]
                )]
            )
        ]
    )
    fun getUserProfile(
        httpRequest: HttpServletRequest
    ): ResponseEntity<ApiResponse<UserProfileResponse>> {
        return try {
            val response = userService.getUserProfile()
            ResponseEntity.ok(ApiResponse.success(response))
        } catch (e: Exception) {
            ResponseEntity.badRequest().body(
                ApiResponse.error(
                    code = ErrorCode.RESOURCE_NOT_FOUND,
                    message = e.message ?: "프로필 조회에 실패했습니다",
                    path = httpRequest.requestURI
                )
            )
        }
    }
    
    @PutMapping("/profile")
    @Operation(
        summary = "프로필 수정",
        description = "사용자의 닉네임과 프로필 이미지를 수정합니다."
    )
    @ApiResponses(
        value = [
            SwaggerApiResponse(
                responseCode = "200",
                description = "프로필 수정 성공",
                content = [Content(
                    mediaType = "application/json",
                    schema = Schema(implementation = ProfileEditResponse::class),
                    examples = [ExampleObject(
                        value = """
                        {
                          "success": true,
                          "message": "프로필이 성공적으로 수정되었습니다.",
                          "data": {
                            "user": {
                              "id": "1",
                              "nickname": "새닉네임",
                              "profileImage": "https://example.com/new-profile.jpg"
                            }
                          }
                        }
                        """
                    )]
                )]
            ),
            SwaggerApiResponse(
                responseCode = "400",
                description = "이미 사용 중인 닉네임"
            )
        ]
    )
    fun editProfile(
        @Parameter(description = "프로필 수정 요청", required = true)
        @Valid @RequestBody request: ProfileEditRequest,
        httpRequest: HttpServletRequest
    ): ResponseEntity<ApiResponse<ProfileEditResponse>> {
        return try {
            val response = userService.editProfile(request)
            ResponseEntity.ok(
                ApiResponse.success(
                    data = response,
                    message = "프로필이 성공적으로 수정되었습니다."
                )
            )
        } catch (e: Exception) {
            val errorCode = when {
                e.message?.contains("닉네임") == true -> ErrorCode.DUPLICATE_NICKNAME
                else -> ErrorCode.VALIDATION_ERROR
            }
            ResponseEntity.badRequest().body(
                ApiResponse.error(
                    code = errorCode,
                    message = e.message ?: "프로필 수정에 실패했습니다",
                    path = httpRequest.requestURI
                )
            )
        }
    }
}