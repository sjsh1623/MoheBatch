package com.mohe.spring.controller;

import com.mohe.spring.dto.ApiResponse;
import com.mohe.spring.service.WeatherService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
// Import removed - using fully qualified name for ApiResponse to avoid conflict
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/weather")
@Tag(name = "날씨 정보", description = "현재 날씨 및 날씨 기반 장소 추천 API")
public class WeatherController {
    
    private final WeatherService weatherService;
    
    public WeatherController(WeatherService weatherService) {
        this.weatherService = weatherService;
    }
    
    @GetMapping("/current")
    @Operation(
        summary = "현재 날씨 조회",
        description = "지정된 위치의 현재 날씨 정보를 조회합니다."
    )
    @ApiResponses(
        value = {
            @io.swagger.v3.oas.annotations.responses.ApiResponse(
                responseCode = "200",
                description = "날씨 조회 성공"
            ),
            @io.swagger.v3.oas.annotations.responses.ApiResponse(
                responseCode = "400",
                description = "잘못된 위치 정보"
            )
        }
    )
    public ResponseEntity<ApiResponse<Object>> getCurrentWeather(
            @Parameter(description = "위도", required = true, example = "37.5665")
            @RequestParam double latitude,
            @Parameter(description = "경도", required = true, example = "126.9780")
            @RequestParam double longitude,
            HttpServletRequest httpRequest) {
        try {
            Object weatherData = weatherService.getCurrentWeather(latitude, longitude);
            return ResponseEntity.ok(ApiResponse.success(weatherData));
        } catch (Exception e) {
            return ResponseEntity.badRequest().body(
                ApiResponse.error(
                    "WEATHER_ERROR",
                    e.getMessage() != null ? e.getMessage() : "날씨 정보 조회에 실패했습니다",
                    httpRequest.getRequestURI()
                )
            );
        }
    }
}