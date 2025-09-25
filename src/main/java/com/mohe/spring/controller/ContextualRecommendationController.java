package com.mohe.spring.controller;

import com.mohe.spring.dto.ApiResponse;
import com.mohe.spring.service.ContextualRecommendationService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/contextual-recommendations")
@Tag(name = "상황별 추천", description = "날씨, 시간, 위치 기반 상황별 장소 추천 API")
public class ContextualRecommendationController {
    
    private final ContextualRecommendationService contextualRecommendationService;
    
    public ContextualRecommendationController(ContextualRecommendationService contextualRecommendationService) {
        this.contextualRecommendationService = contextualRecommendationService;
    }
    
    @GetMapping("/weather-based")
    @Operation(
        summary = "날씨 기반 장소 추천",
        description = "현재 날씨 상황에 적합한 장소를 추천합니다."
    )
    public ResponseEntity<ApiResponse<Object>> getWeatherBasedRecommendations(
            @Parameter(description = "위도", required = true)
            @RequestParam double latitude,
            @Parameter(description = "경도", required = true)
            @RequestParam double longitude,
            HttpServletRequest httpRequest) {
        try {
            Object recommendations = contextualRecommendationService.getWeatherBasedRecommendations(latitude, longitude);
            return ResponseEntity.ok(ApiResponse.success(recommendations));
        } catch (Exception e) {
            return ResponseEntity.badRequest().body(
                ApiResponse.error(
                    "RECOMMENDATION_ERROR",
                    e.getMessage() != null ? e.getMessage() : "날씨 기반 추천에 실패했습니다",
                    httpRequest.getRequestURI()
                )
            );
        }
    }
}