package com.mohe.spring.controller;

import com.mohe.spring.dto.ApiResponse;
import com.mohe.spring.service.ImageManagementService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/images")
@Tag(name = "이미지 관리", description = "장소 이미지 관리 API")
public class ImageManagementController {
    
    private final ImageManagementService imageManagementService;
    
    public ImageManagementController(ImageManagementService imageManagementService) {
        this.imageManagementService = imageManagementService;
    }
    
    @PostMapping("/place/{placeId}/fetch")
    @PreAuthorize("hasRole('ADMIN')")
    @Operation(
        summary = "장소 이미지 가져오기",
        description = "외부 API에서 장소 이미지를 가져와서 저장합니다."
    )
    public ResponseEntity<ApiResponse<Object>> fetchPlaceImages(
            @Parameter(description = "장소 ID", required = true)
            @PathVariable Long placeId,
            HttpServletRequest httpRequest) {
        try {
            Object result = imageManagementService.fetchImagesForPlace(placeId);
            return ResponseEntity.ok(ApiResponse.success(result));
        } catch (Exception e) {
            return ResponseEntity.badRequest().body(
                ApiResponse.error(
                    "IMAGE_FETCH_ERROR",
                    e.getMessage() != null ? e.getMessage() : "이미지 가져오기에 실패했습니다",
                    httpRequest.getRequestURI()
                )
            );
        }
    }
}