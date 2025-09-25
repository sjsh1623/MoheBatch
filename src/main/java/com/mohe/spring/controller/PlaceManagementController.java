package com.mohe.spring.controller;

import com.mohe.spring.dto.ApiResponse;
import com.mohe.spring.service.PlaceManagementService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/admin/place-management")
@Tag(name = "장소 관리", description = "장소 데이터 관리를 위한 관리자 API")
@PreAuthorize("hasRole('ADMIN')")
public class PlaceManagementController {
    
    private final PlaceManagementService placeManagementService;
    
    public PlaceManagementController(PlaceManagementService placeManagementService) {
        this.placeManagementService = placeManagementService;
    }
    
    @PostMapping("/check-availability")
    @Operation(
        summary = "장소 데이터 가용성 확인",
        description = "추천 가능한 장소의 개수를 확인하고 부족한 경우 자동으로 수집을 시작합니다."
    )
    public ResponseEntity<ApiResponse<Object>> checkPlaceAvailability(
            @Parameter(description = "최소 필요한 장소 수", example = "50")
            @RequestParam(defaultValue = "50") int minRequired,
            HttpServletRequest httpRequest) {
        try {
            Object result = placeManagementService.checkAndEnsureAvailability(minRequired);
            return ResponseEntity.ok(ApiResponse.success(result));
        } catch (Exception e) {
            return ResponseEntity.badRequest().body(
                ApiResponse.error(
                    "AVAILABILITY_CHECK_ERROR",
                    e.getMessage() != null ? e.getMessage() : "장소 가용성 확인에 실패했습니다",
                    httpRequest.getRequestURI()
                )
            );
        }
    }
    
    @PostMapping("/fetch")
    @Operation(
        summary = "장소 데이터 수집",
        description = "외부 API를 통해 새로운 장소 데이터를 수집합니다."
    )
    public ResponseEntity<ApiResponse<Object>> fetchPlaces(
            @Parameter(description = "목표 수집 개수", example = "100")
            @RequestParam(defaultValue = "100") int targetCount,
            @Parameter(description = "카테고리 필터", example = "카페")
            @RequestParam(required = false) String category,
            HttpServletRequest httpRequest) {
        try {
            Object result = placeManagementService.fetchNewPlaces(targetCount, category);
            return ResponseEntity.ok(ApiResponse.success(result));
        } catch (Exception e) {
            return ResponseEntity.badRequest().body(
                ApiResponse.error(
                    "PLACE_FETCH_ERROR",
                    e.getMessage() != null ? e.getMessage() : "장소 데이터 수집에 실패했습니다",
                    httpRequest.getRequestURI()
                )
            );
        }
    }
    
    @PostMapping("/cleanup")
    @Operation(
        summary = "오래된 장소 데이터 정리",
        description = "오래되고 평점이 낮은 장소 데이터를 정리합니다."
    )
    public ResponseEntity<ApiResponse<Object>> cleanupOldPlaces(
            @Parameter(description = "확인할 최대 장소 수", example = "50")
            @RequestParam(defaultValue = "50") int maxPlacesToCheck,
            HttpServletRequest httpRequest) {
        try {
            Object result = placeManagementService.cleanupOldLowRatedPlaces(maxPlacesToCheck);
            return ResponseEntity.ok(ApiResponse.success(result));
        } catch (Exception e) {
            return ResponseEntity.badRequest().body(
                ApiResponse.error(
                    "CLEANUP_ERROR",
                    e.getMessage() != null ? e.getMessage() : "장소 데이터 정리에 실패했습니다",
                    httpRequest.getRequestURI()
                )
            );
        }
    }
}