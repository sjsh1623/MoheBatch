package com.mohe.spring.controller;

import com.mohe.spring.dto.*;
import com.mohe.spring.service.UserService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.ExampleObject;
import io.swagger.v3.oas.annotations.media.Schema;
// Import removed - using fully qualified name for ApiResponse to avoid conflict
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.security.SecurityRequirement;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.validation.Valid;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/user")
@PreAuthorize("hasRole('USER')")
@SecurityRequirement(name = "bearerAuth")
@Tag(name = "사용자 관리", description = "사용자 프로필 및 선호도 관리 API")
public class UserController {
    
    private final UserService userService;
    
    public UserController(UserService userService) {
        this.userService = userService;
    }
    
    @PutMapping("/preferences")
    @Operation(
        summary = "사용자 선호도 설정",
        description = "MBTI, 연령대, 공간 선호도, 교통수단 등 사용자 선호도를 설정합니다."
    )
    @ApiResponses(
        value = {
            @io.swagger.v3.oas.annotations.responses.ApiResponse(
                responseCode = "200",
                description = "선호도 설정 성공",
                content = @Content(
                    mediaType = "application/json",
                    schema = @Schema(implementation = UserPreferencesResponse.class),
                    examples = @ExampleObject(
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
                    )
                )
            )
        }
    )
    public ResponseEntity<ApiResponse<UserPreferencesResponse>> updatePreferences(
            @Parameter(description = "사용자 선호도 설정 요청", required = true)
            @Valid @RequestBody UserPreferencesRequest request,
            HttpServletRequest httpRequest) {
        try {
            UserPreferencesResponse response = userService.updatePreferences(request);
            return ResponseEntity.ok(
                ApiResponse.success(
                    response,
                    "선호도 설정이 완료되었습니다."
                )
            );
        } catch (Exception e) {
            return ResponseEntity.badRequest().body(
                ApiResponse.error(
                    ErrorCode.VALIDATION_ERROR,
                    e.getMessage() != null ? e.getMessage() : "선호도 설정에 실패했습니다",
                    httpRequest.getRequestURI()
                )
            );
        }
    }
    
    @GetMapping("/profile")
    @Operation(
        summary = "사용자 프로필 조회",
        description = "현재 로그인된 사용자의 프로필 정보를 조회합니다."
    )
    @ApiResponses(
        value = {
            @io.swagger.v3.oas.annotations.responses.ApiResponse(
                responseCode = "200",
                description = "프로필 조회 성공",
                content = @Content(
                    mediaType = "application/json",
                    schema = @Schema(implementation = UserProfileResponse.class),
                    examples = @ExampleObject(
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
                    )
                )
            )
        }
    )
    public ResponseEntity<ApiResponse<UserProfileResponse>> getUserProfile(
            HttpServletRequest httpRequest) {
        try {
            UserProfileResponse response = userService.getUserProfile();
            return ResponseEntity.ok(ApiResponse.success(response));
        } catch (Exception e) {
            return ResponseEntity.badRequest().body(
                ApiResponse.error(
                    ErrorCode.RESOURCE_NOT_FOUND,
                    e.getMessage() != null ? e.getMessage() : "프로필 조회에 실패했습니다",
                    httpRequest.getRequestURI()
                )
            );
        }
    }
    
    @PutMapping("/profile")
    @Operation(
        summary = "프로필 수정",
        description = "사용자의 닉네임과 프로필 이미지를 수정합니다."
    )
    @ApiResponses(
        value = {
            @io.swagger.v3.oas.annotations.responses.ApiResponse(
                responseCode = "200",
                description = "프로필 수정 성공",
                content = @Content(
                    mediaType = "application/json",
                    schema = @Schema(implementation = ProfileEditResponse.class),
                    examples = @ExampleObject(
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
                    )
                )
            ),
            @io.swagger.v3.oas.annotations.responses.ApiResponse(
                responseCode = "400",
                description = "이미 사용 중인 닉네임"
            )
        }
    )
    public ResponseEntity<ApiResponse<ProfileEditResponse>> editProfile(
            @Parameter(description = "프로필 수정 요청", required = true)
            @Valid @RequestBody ProfileEditRequest request,
            HttpServletRequest httpRequest) {
        try {
            ProfileEditResponse response = userService.editProfile(request);
            return ResponseEntity.ok(
                ApiResponse.success(
                    response,
                    "프로필이 성공적으로 수정되었습니다."
                )
            );
        } catch (Exception e) {
            String errorCode;
            if (e.getMessage() != null && e.getMessage().contains("닉네임")) {
                errorCode = ErrorCode.DUPLICATE_NICKNAME;
            } else {
                errorCode = ErrorCode.VALIDATION_ERROR;
            }
            return ResponseEntity.badRequest().body(
                ApiResponse.error(
                    errorCode,
                    e.getMessage() != null ? e.getMessage() : "프로필 수정에 실패했습니다",
                    httpRequest.getRequestURI()
                )
            );
        }
    }
}