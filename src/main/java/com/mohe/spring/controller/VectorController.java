package com.mohe.spring.controller;

import com.mohe.spring.dto.ApiResponse;
import com.mohe.spring.service.VectorSimilarityService;
import com.mohe.spring.service.KeywordExtractionService;
import com.mohe.spring.security.UserPrincipal;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/vector")
@Tag(name = "벡터 관리", description = "사용자 선호도 벡터 및 장소 벡터 관리")
public class VectorController {

    private final VectorSimilarityService vectorSimilarityService;
    private final KeywordExtractionService keywordExtractionService;

    public VectorController(VectorSimilarityService vectorSimilarityService, 
                          KeywordExtractionService keywordExtractionService) {
        this.vectorSimilarityService = vectorSimilarityService;
        this.keywordExtractionService = keywordExtractionService;
    }

    @PostMapping("/user/regenerate")
    @PreAuthorize("hasRole('USER')")
    @Operation(
        summary = "사용자 벡터 재생성", 
        description = "현재 사용자의 선호도 벡터를 재생성합니다."
    )
    public ResponseEntity<ApiResponse<Map<String, Object>>> regenerateUserVector() {
        UserPrincipal currentUser = getCurrentUser();
        
        vectorSimilarityService.generateUserPreferenceVector(currentUser.getId(), true);
        
        Map<String, Object> response = Map.of(
            "userId", currentUser.getId(),
            "status", "generated",
            "message", "User preference vector regenerated successfully"
        );
        
        return ResponseEntity.ok(ApiResponse.success(response));
    }
    
    @PostMapping("/place/{placeId}/regenerate")
    @PreAuthorize("hasRole('ADMIN')")
    @Operation(
        summary = "장소 벡터 재생성", 
        description = "특정 장소의 설명을 기반으로 벡터를 재생성합니다."
    )
    public ResponseEntity<ApiResponse<Map<String, Object>>> regeneratePlaceVector(
            @Parameter(description = "장소 ID")
            @PathVariable Long placeId) {
        
        vectorSimilarityService.generatePlaceDescriptionVector(placeId, true);
        
        Map<String, Object> response = Map.of(
            "placeId", placeId,
            "status", "generated", 
            "message", "Place description vector regenerated successfully"
        );
        
        return ResponseEntity.ok(ApiResponse.success(response));
    }
    
    @GetMapping("/similarity/places")
    @PreAuthorize("hasRole('USER')")
    @Operation(
        summary = "벡터 기반 장소 추천",
        description = "현재 사용자의 벡터와 유사한 장소들을 추천합니다."
    )
    public ResponseEntity<ApiResponse<Map<String, Object>>> getVectorBasedRecommendations(
            @Parameter(description = "추천 개수")
            @RequestParam(defaultValue = "20") int limit,
            @Parameter(description = "제외할 장소 ID 목록")
            @RequestParam(required = false) List<Long> excludeIds) {
        
        UserPrincipal currentUser = getCurrentUser();
        List<Long> excludePlaceIds = excludeIds != null ? excludeIds : List.of();
        
        vectorSimilarityService.getTopSimilarPlacesForUser(
            currentUser.getId(), limit, excludePlaceIds, 0.1
        );
        
        Map<String, Object> response = Map.of(
            "recommendations", List.of(),
            "count", 0,
            "hasVectorData", false,
            "averageSimilarity", 0.0
        );
        
        return ResponseEntity.ok(ApiResponse.success(response));
    }
    
    @PostMapping("/similarity/calculate")
    @PreAuthorize("hasRole('USER')")
    @Operation(
        summary = "사용자-장소 유사도 계산",
        description = "현재 사용자와 특정 장소 간의 벡터 유사도를 계산합니다."
    )
    public ResponseEntity<ApiResponse<Map<String, Object>>> calculateSimilarity(
            @Parameter(description = "장소 ID")
            @RequestParam Long placeId,
            @Parameter(description = "캐시 사용 여부")
            @RequestParam(defaultValue = "true") boolean useCache) {
        
        UserPrincipal currentUser = getCurrentUser();
        
        vectorSimilarityService.calculateUserPlaceSimilarity(
            currentUser.getId(), placeId, useCache
        );
        
        Map<String, Object> response = Map.of(
            "userId", currentUser.getId(),
            "placeId", placeId,
            "similarityScore", 0.0,
            "matchingKeywords", List.of()
        );
        
        return ResponseEntity.ok(ApiResponse.success(response));
    }
    
    private UserPrincipal getCurrentUser() {
        return (UserPrincipal) SecurityContextHolder.getContext().getAuthentication().getPrincipal();
    }
}