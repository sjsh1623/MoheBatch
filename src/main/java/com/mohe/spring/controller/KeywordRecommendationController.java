package com.mohe.spring.controller;

import com.mohe.spring.dto.ApiResponse;
import com.mohe.spring.service.KeywordRecommendationService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/keyword-recommendations")
@Tag(name = "키워드 추천", description = "키워드 기반 장소 추천 API")
public class KeywordRecommendationController {
    
    private final KeywordRecommendationService keywordRecommendationService;
    
    public KeywordRecommendationController(KeywordRecommendationService keywordRecommendationService) {
        this.keywordRecommendationService = keywordRecommendationService;
    }
    
    @GetMapping("/by-keyword")
    @PreAuthorize("hasRole('USER')")
    @Operation(
        summary = "키워드 기반 장소 추천",
        description = "지정된 키워드와 유사한 장소들을 추천합니다."
    )
    public ResponseEntity<ApiResponse<Object>> getRecommendationsByKeyword(
            @Parameter(description = "검색 키워드", required = true)
            @RequestParam String keyword,
            @Parameter(description = "추천 개수", example = "10")
            @RequestParam(defaultValue = "10") int limit,
            HttpServletRequest httpRequest) {
        try {
            Object recommendations = keywordRecommendationService.getRecommendationsByKeyword(keyword, limit);
            return ResponseEntity.ok(ApiResponse.success(recommendations));
        } catch (Exception e) {
            return ResponseEntity.badRequest().body(
                ApiResponse.error(
                    "KEYWORD_RECOMMENDATION_ERROR",
                    e.getMessage() != null ? e.getMessage() : "키워드 기반 추천에 실패했습니다",
                    httpRequest.getRequestURI()
                )
            );
        }
    }
}