package com.mohe.spring.controller

import com.mohe.spring.dto.*
import com.mohe.spring.service.ActivityService
import io.swagger.v3.oas.annotations.Operation
import io.swagger.v3.oas.annotations.media.Content
import io.swagger.v3.oas.annotations.media.ExampleObject
import io.swagger.v3.oas.annotations.media.Schema
import io.swagger.v3.oas.annotations.responses.ApiResponse as SwaggerApiResponse
import io.swagger.v3.oas.annotations.responses.ApiResponses
import io.swagger.v3.oas.annotations.security.SecurityRequirement
import io.swagger.v3.oas.annotations.tags.Tag
import jakarta.servlet.http.HttpServletRequest
import org.springframework.http.ResponseEntity
import org.springframework.security.access.prepost.PreAuthorize
import org.springframework.web.bind.annotation.*

@RestController
@RequestMapping("/api/user")
@PreAuthorize("hasRole('USER')")
@SecurityRequirement(name = "bearerAuth")
@Tag(name = "사용자 활동", description = "최근 조회 장소, 등록한 장소 등 사용자 활동 관리 API")
class ActivityController(
    private val activityService: ActivityService
) {
    
    @GetMapping("/recent-places")
    @Operation(
        summary = "최근 조회한 장소 목록",
        description = "사용자가 최근에 조회한 장소들의 목록을 조회합니다."
    )
    @ApiResponses(
        value = [
            SwaggerApiResponse(
                responseCode = "200",
                description = "최근 조회 장소 목록 조회 성공",
                content = [Content(
                    mediaType = "application/json",
                    schema = Schema(implementation = RecentPlacesResponse::class),
                    examples = [ExampleObject(
                        value = """
                        {
                          "success": true,
                          "data": {
                            "recentPlaces": [
                              {
                                "id": "1",
                                "title": "카페 무브먼트랩",
                                "location": "서울 성수동",
                                "image": "https://example.com/place.jpg",
                                "rating": 4.7,
                                "viewedAt": "2024-01-01T12:00:00Z"
                              }
                            ]
                          }
                        }
                        """
                    )]
                )]
            )
        ]
    )
    fun getRecentPlaces(
        httpRequest: HttpServletRequest
    ): ResponseEntity<ApiResponse<RecentPlacesResponse>> {
        return try {
            val response = activityService.getRecentPlaces()
            ResponseEntity.ok(ApiResponse.success(response))
        } catch (e: Exception) {
            ResponseEntity.badRequest().body(
                ApiResponse.error(
                    code = ErrorCode.INTERNAL_SERVER_ERROR,
                    message = e.message ?: "최근 조회한 장소 목록 조회에 실패했습니다",
                    path = httpRequest.requestURI
                )
            )
        }
    }
    
    @GetMapping("/my-places")
    @Operation(
        summary = "내가 등록한 장소 목록",
        description = "사용자가 직접 등록한 장소들의 목록을 조회합니다."
    )
    @ApiResponses(
        value = [
            SwaggerApiResponse(
                responseCode = "200",
                description = "내가 등록한 장소 목록 조회 성공",
                content = [Content(
                    mediaType = "application/json",
                    schema = Schema(implementation = MyPlacesResponse::class),
                    examples = [ExampleObject(
                        value = """
                        {
                          "success": true,
                          "data": {
                            "myPlaces": [
                              {
                                "id": "1",
                                "title": "내가 추천하는 카페",
                                "location": "서울 강남구",
                                "image": "https://example.com/my-place.jpg",
                                "status": "approved",
                                "createdAt": "2024-01-01T00:00:00Z"
                              }
                            ]
                          }
                        }
                        """
                    )]
                )]
            )
        ]
    )
    fun getMyPlaces(
        httpRequest: HttpServletRequest
    ): ResponseEntity<ApiResponse<MyPlacesResponse>> {
        return try {
            val response = activityService.getMyPlaces()
            ResponseEntity.ok(ApiResponse.success(response))
        } catch (e: Exception) {
            ResponseEntity.badRequest().body(
                ApiResponse.error(
                    code = ErrorCode.INTERNAL_SERVER_ERROR,
                    message = e.message ?: "등록한 장소 목록 조회에 실패했습니다",
                    path = httpRequest.requestURI
                )
            )
        }
    }
}