package com.mohe.spring.controller;

import com.mohe.spring.dto.ApiResponse;
import com.mohe.spring.service.PlaceEnhancementService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/place-enhancement")
@Tag(name = "장소 정보 강화", description = "장소 데이터 품질 향상을 위한 API")
public class PlaceEnhancementController {
    
    private final PlaceEnhancementService placeEnhancementService;
    
    public PlaceEnhancementController(PlaceEnhancementService placeEnhancementService) {
        this.placeEnhancementService = placeEnhancementService;
    }
    
    @PostMapping("/place/{placeId}/enhance")
    @PreAuthorize("hasRole('ADMIN')")
    @Operation(
        summary = "장소 정보 강화",
        description = "외부 API를 통해 장소 정보를 강화합니다 (평점, 리뷰, 이미지 등)."
    )
    public ResponseEntity<ApiResponse<Object>> enhancePlace(
            @Parameter(description = "장소 ID", required = true)
            @PathVariable Long placeId,
            HttpServletRequest httpRequest) {
        try {
            Object result = placeEnhancementService.enhancePlace(placeId);
            return ResponseEntity.ok(ApiResponse.success(result));
        } catch (Exception e) {
            return ResponseEntity.badRequest().body(
                ApiResponse.error(
                    "PLACE_ENHANCEMENT_ERROR",
                    e.getMessage() != null ? e.getMessage() : "장소 정보 강화에 실패했습니다",
                    httpRequest.getRequestURI()
                )
            );
        }
    }
    
    @PostMapping("/batch-enhance")
    @PreAuthorize("hasRole('ADMIN')")
    @Operation(
        summary = "배치 장소 정보 강화",
        description = "여러 장소의 정보를 일괄적으로 강화합니다."
    )
    public ResponseEntity<ApiResponse<Object>> batchEnhancePlaces(
            @Parameter(description = "강화할 장소 수", example = "100")
            @RequestParam(defaultValue = "100") int limit,
            HttpServletRequest httpRequest) {
        try {
            Object result = placeEnhancementService.batchEnhancePlaces(limit);
            return ResponseEntity.ok(ApiResponse.success(result));
        } catch (Exception e) {
            return ResponseEntity.badRequest().body(
                ApiResponse.error(
                    "BATCH_ENHANCEMENT_ERROR",
                    e.getMessage() != null ? e.getMessage() : "배치 장소 정보 강화에 실패했습니다",
                    httpRequest.getRequestURI()
                )
            );
        }
    }
}