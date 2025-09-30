package com.mohe.spring.controller;

import com.mohe.spring.dto.*;
import com.mohe.spring.service.ActivityService;
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
@Tag(name = "사용자 활동", description = "최근 조회 장소, 등록한 장소 등 사용자 활동 관리 API")
public class ActivityController {
    
    private final ActivityService activityService;
    
    public ActivityController(ActivityService activityService) {
        this.activityService = activityService;
    }
    
    @GetMapping("/recent-places")
    @Operation(
        summary = "최근 조회한 장소 목록",
        description = "사용자가 최근에 조회한 장소들의 목록을 조회합니다."
    )
    @ApiResponses(
        value = {
            @io.swagger.v3.oas.annotations.responses.ApiResponse(
                responseCode = "200",
                description = "최근 조회 장소 목록 조회 성공",
                content = @Content(
                    mediaType = "application/json",
                    schema = @Schema(implementation = RecentPlacesResponse.class),
                    examples = @ExampleObject(
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
                    )
                )
            )
        }
    )
    public ResponseEntity<ApiResponse<RecentPlacesResponse>> getRecentPlaces(
            HttpServletRequest httpRequest) {
        try {
            RecentPlacesResponse response = activityService.getRecentPlaces();
            return ResponseEntity.ok(ApiResponse.success(response));
        } catch (Exception e) {
            return ResponseEntity.badRequest().body(
                ApiResponse.error(
                    ErrorCode.INTERNAL_SERVER_ERROR,
                    e.getMessage() != null ? e.getMessage() : "최근 조회한 장소 목록 조회에 실패했습니다",
                    httpRequest.getRequestURI()
                )
            );
        }
    }

    @PostMapping("/recent-places")
    @Operation(
        summary = "최근 조회한 장소 기록",
        description = "사용자가 장소 상세를 확인할 때 최근 조회 이력으로 저장합니다."
    )
    public ResponseEntity<ApiResponse<Void>> recordRecentPlace(
            @Parameter(description = "최근 조회 장소 요청", required = true)
            @Valid @RequestBody RecentViewRequest request,
            HttpServletRequest httpRequest) {
        try {
            activityService.recordRecentPlaceView(request.getPlaceId());
            return ResponseEntity.ok(ApiResponse.success("최근 조회 이력이 저장되었습니다."));
        } catch (Exception e) {
            return ResponseEntity.badRequest().body(
                ApiResponse.error(
                    ErrorCode.INTERNAL_SERVER_ERROR,
                    e.getMessage() != null ? e.getMessage() : "최근 조회 이력 저장에 실패했습니다",
                    httpRequest.getRequestURI()
                )
            );
        }
    }
    
    @GetMapping("/my-places")
    @Operation(
        summary = "내가 등록한 장소 목록",
        description = "사용자가 직접 등록한 장소들의 목록을 조회합니다."
    )
    @ApiResponses(
        value = {
            @io.swagger.v3.oas.annotations.responses.ApiResponse(
                responseCode = "200",
                description = "내가 등록한 장소 목록 조회 성공",
                content = @Content(
                    mediaType = "application/json",
                    schema = @Schema(implementation = MyPlacesResponse.class),
                    examples = @ExampleObject(
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
                    )
                )
            )
        }
    )
    public ResponseEntity<ApiResponse<MyPlacesResponse>> getMyPlaces(
            HttpServletRequest httpRequest) {
        try {
            MyPlacesResponse response = activityService.getMyPlaces();
            return ResponseEntity.ok(ApiResponse.success(response));
        } catch (Exception e) {
            return ResponseEntity.badRequest().body(
                ApiResponse.error(
                    ErrorCode.INTERNAL_SERVER_ERROR,
                    e.getMessage() != null ? e.getMessage() : "등록한 장소 목록 조회에 실패했습니다",
                    httpRequest.getRequestURI()
                )
            );
        }
    }
}
