package com.mohe.spring.controller;

import com.mohe.spring.dto.ApiResponse;
import com.mohe.spring.service.ImageManagementService;
import com.mohe.spring.service.GeminiImageService;
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
    private final GeminiImageService geminiImageService;
    
    public ImageManagementController(ImageManagementService imageManagementService, GeminiImageService geminiImageService) {
        this.imageManagementService = imageManagementService;
        this.geminiImageService = geminiImageService;
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
    
    @PostMapping("/place/{placeId}/generate")
    @PreAuthorize("hasRole('ADMIN')")
    @Operation(
        summary = "장소 이미지 생성",
        description = "Gemini API를 사용하여 장소 이미지를 생성합니다."
    )
    public ResponseEntity<ApiResponse<Object>> generatePlaceImage(
            @Parameter(description = "장소 ID", required = true)
            @PathVariable Long placeId,
            @Parameter(description = "장소 이름", required = false)
            @RequestParam(required = false) String placeName,
            @Parameter(description = "장소 설명", required = false)
            @RequestParam(required = false) String placeDescription,
            @Parameter(description = "장소 카테고리", required = false)
            @RequestParam(required = false) String placeCategory,
            HttpServletRequest httpRequest) {
        try {
            // If place info not provided, fetch from database
            if (placeName == null || placeDescription == null || placeCategory == null) {
                // TODO: Fetch place details from database using placeId
                placeName = placeName != null ? placeName : "Sample Place " + placeId;
                placeDescription = placeDescription != null ? placeDescription : "A wonderful place to visit";
                placeCategory = placeCategory != null ? placeCategory : "카페";
            }
            
            String imageUrl = geminiImageService.generatePlaceImage(placeName, placeDescription, placeCategory);
            
            if (imageUrl != null) {
                return ResponseEntity.ok(ApiResponse.success(
                    java.util.Map.of(
                        "placeId", placeId,
                        "imageUrl", imageUrl,
                        "placeName", placeName,
                        "status", "generated"
                    )
                ));
            } else {
                return ResponseEntity.badRequest().body(
                    ApiResponse.error(
                        "IMAGE_GENERATION_ERROR",
                        "이미지 생성에 실패했습니다",
                        httpRequest.getRequestURI()
                    )
                );
            }
        } catch (Exception e) {
            return ResponseEntity.badRequest().body(
                ApiResponse.error(
                    "IMAGE_GENERATION_ERROR",
                    e.getMessage() != null ? e.getMessage() : "이미지 생성에 실패했습니다",
                    httpRequest.getRequestURI()
                )
            );
        }
    }
}