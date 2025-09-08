package com.mohe.spring.controller;

import com.mohe.spring.dto.ApiResponse;
import com.mohe.spring.dto.KoreanRegionDto;
import com.mohe.spring.service.KoreanGovernmentApiService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Controller for accessing Korean Government Administrative Region Data
 * Provides endpoints to fetch Korean administrative regions temporarily (not stored in database)
 */
@RestController
@RequestMapping("/api/korean-regions")
@Tag(name = "Korean Regions", description = "Korean Government Administrative Region API")
public class KoreanRegionController {
    
    private final KoreanGovernmentApiService koreanGovernmentApiService;
    
    @Autowired
    public KoreanRegionController(KoreanGovernmentApiService koreanGovernmentApiService) {
        this.koreanGovernmentApiService = koreanGovernmentApiService;
    }
    
    /**
     * Get all Korean administrative regions from government API
     */
    @GetMapping("/all")
    @Operation(
        summary = "모든 한국 행정구역 조회",
        description = "한국 정부 표준지역코드 API로부터 모든 행정구역 정보를 조회합니다 (임시 데이터, DB 저장 안됨)"
    )
    @io.swagger.v3.oas.annotations.responses.ApiResponse(responseCode = "200", description = "행정구역 조회 성공")
    @io.swagger.v3.oas.annotations.responses.ApiResponse(responseCode = "500", description = "API 호출 실패")
    public ResponseEntity<ApiResponse<List<KoreanRegionDto>>> getAllRegions() {
        try {
            List<KoreanRegionDto> regions = koreanGovernmentApiService.fetchAllKoreanRegions();
            
            Map<String, Object> metadata = new HashMap<>();
            metadata.put("totalCount", regions.size());
            metadata.put("cacheStatus", koreanGovernmentApiService.getCacheStatus());
            metadata.put("dataSource", "Korean Government Administrative Standard Code API");
            metadata.put("note", "Data fetched temporarily, not stored in database");
            
            return ResponseEntity.ok(ApiResponse.success(regions));
        } catch (Exception e) {
            return ResponseEntity.internalServerError()
                .body(ApiResponse.error("KOREAN_REGIONS_ERROR", "Failed to fetch Korean administrative regions: " + e.getMessage()));
        }
    }
    
    /**
     * Get only dong-level Korean administrative regions
     */
    @GetMapping("/dong-level")
    @Operation(
        summary = "동 단위 한국 행정구역 조회", 
        description = "동/읍/면 단위의 행정구역만 필터링하여 조회합니다"
    )
    @io.swagger.v3.oas.annotations.responses.ApiResponse(responseCode = "200", description = "동 단위 행정구역 조회 성공")
    public ResponseEntity<ApiResponse<List<KoreanRegionDto>>> getDongLevelRegions() {
        try {
            List<KoreanRegionDto> regions = koreanGovernmentApiService.fetchDongLevelRegions();
            
            Map<String, Object> metadata = new HashMap<>();
            metadata.put("dongLevelCount", regions.size());
            metadata.put("filter", "umdCode != '00' AND riCode == '00'");
            metadata.put("cacheStatus", koreanGovernmentApiService.getCacheStatus());
            
            return ResponseEntity.ok(ApiResponse.success(regions));
        } catch (Exception e) {
            return ResponseEntity.internalServerError()
                .body(ApiResponse.error("DONG_LEVEL_ERROR", "Failed to fetch dong-level regions: " + e.getMessage()));
        }
    }
    
    /**
     * Get location names for search queries
     */
    @GetMapping("/search-locations")
    @Operation(
        summary = "검색용 지역명 목록 조회",
        description = "API 검색에 사용할 수 있는 간단한 지역명 목록을 조회합니다 (예: '강남구', '신사동')"
    )
    @io.swagger.v3.oas.annotations.responses.ApiResponse(responseCode = "200", description = "검색용 지역명 조회 성공")
    public ResponseEntity<ApiResponse<List<String>>> getSearchLocationNames() {
        try {
            List<String> locationNames = koreanGovernmentApiService.fetchLocationNamesForSearch();
            
            Map<String, Object> metadata = new HashMap<>();
            metadata.put("uniqueLocationCount", locationNames.size());
            metadata.put("usage", "Use these names for Naver/Google API location searches");
            metadata.put("sampleLocations", locationNames.size() > 10 ? 
                locationNames.subList(0, 10) : locationNames);
            
            return ResponseEntity.ok(ApiResponse.success(locationNames));
        } catch (Exception e) {
            return ResponseEntity.internalServerError()
                .body(ApiResponse.error("SEARCH_LOCATIONS_ERROR", "Failed to fetch search location names: " + e.getMessage()));
        }
    }
    
    /**
     * Get regions by sido (province)
     */
    @GetMapping("/by-sido")
    @Operation(
        summary = "시도별 행정구역 조회",
        description = "특정 시도의 행정구역만 필터링하여 조회합니다"
    )
    @io.swagger.v3.oas.annotations.responses.ApiResponse(responseCode = "200", description = "시도별 행정구역 조회 성공")
    public ResponseEntity<ApiResponse<List<KoreanRegionDto>>> getRegionsBySido(
            @Parameter(description = "시도 코드 (예: 11 - 서울, 26 - 부산)", example = "11")
            @RequestParam String sidoCode) {
        try {
            List<KoreanRegionDto> allRegions = koreanGovernmentApiService.fetchAllKoreanRegions();
            
            List<KoreanRegionDto> filteredRegions = allRegions.stream()
                .filter(region -> sidoCode.equals(region.getSidoCode()))
                .toList();
            
            Map<String, Object> metadata = new HashMap<>();
            metadata.put("sidoCode", sidoCode);
            metadata.put("filteredCount", filteredRegions.size());
            metadata.put("totalAvailable", allRegions.size());
            
            return ResponseEntity.ok(ApiResponse.success(filteredRegions));
        } catch (Exception e) {
            return ResponseEntity.internalServerError()
                .body(ApiResponse.error("SIDO_REGIONS_ERROR", "Failed to fetch regions by sido: " + e.getMessage()));
        }
    }
    
    /**
     * Clear the Korean regions cache to force fresh data
     */
    @PostMapping("/clear-cache")
    @Operation(
        summary = "캐시 초기화",
        description = "한국 행정구역 캐시를 초기화하여 다음 호출시 최신 데이터를 가져옵니다"
    )
    @io.swagger.v3.oas.annotations.responses.ApiResponse(responseCode = "200", description = "캐시 초기화 성공")
    public ResponseEntity<ApiResponse<Void>> clearCache() {
        try {
            koreanGovernmentApiService.clearCache();
            return ResponseEntity.ok(ApiResponse.success("Korean regions cache cleared successfully"));
        } catch (Exception e) {
            return ResponseEntity.internalServerError()
                .body(ApiResponse.error("CACHE_CLEAR_ERROR", "Failed to clear cache: " + e.getMessage()));
        }
    }
    
    /**
     * Get cache status and statistics
     */
    @GetMapping("/cache-status")
    @Operation(
        summary = "캐시 상태 확인",
        description = "현재 한국 행정구역 데이터 캐시 상태를 확인합니다"
    )
    @io.swagger.v3.oas.annotations.responses.ApiResponse(responseCode = "200", description = "캐시 상태 확인 성공")
    public ResponseEntity<ApiResponse<Map<String, Object>>> getCacheStatus() {
        try {
            Map<String, Object> status = new HashMap<>();
            status.put("cacheStatus", koreanGovernmentApiService.getCacheStatus());
            status.put("apiEndpoint", "Korean Government Administrative Standard Code API");
            status.put("dataStorage", "Temporary in-memory cache only, not stored in database");
            
            return ResponseEntity.ok(ApiResponse.success(status));
        } catch (Exception e) {
            return ResponseEntity.internalServerError()
                .body(ApiResponse.error("CACHE_STATUS_ERROR", "Failed to get cache status: " + e.getMessage()));
        }
    }
}