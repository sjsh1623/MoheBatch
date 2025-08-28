package com.mohe.spring.controller

import com.mohe.spring.dto.ApiResponse
import com.mohe.spring.service.WeatherService
import com.mohe.spring.service.WeatherData
import com.mohe.spring.service.WeatherContext
import io.swagger.v3.oas.annotations.Operation
import io.swagger.v3.oas.annotations.Parameter
import io.swagger.v3.oas.annotations.media.Content
import io.swagger.v3.oas.annotations.media.ExampleObject
import io.swagger.v3.oas.annotations.media.Schema
import io.swagger.v3.oas.annotations.responses.ApiResponse as SwaggerApiResponse
import io.swagger.v3.oas.annotations.responses.ApiResponses
import io.swagger.v3.oas.annotations.security.SecurityRequirements
import io.swagger.v3.oas.annotations.tags.Tag
import jakarta.servlet.http.HttpServletRequest
import org.slf4j.LoggerFactory
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.*

@RestController
@RequestMapping("/api/weather")
@SecurityRequirements
@Tag(name = "날씨 정보", description = "위치 기반 날씨 정보 및 컨텍스트 API")
class WeatherController(
    private val weatherService: WeatherService
) {
    private val logger = LoggerFactory.getLogger(WeatherController::class.java)
    
    @GetMapping("/current")
    @Operation(
        summary = "현재 날씨 정보 조회",
        description = "지정된 위치의 현재 날씨 정보를 조회합니다."
    )
    @ApiResponses(
        value = [
            SwaggerApiResponse(
                responseCode = "200",
                description = "날씨 정보 조회 성공",
                content = [Content(
                    mediaType = "application/json",
                    schema = Schema(implementation = WeatherData::class),
                    examples = [ExampleObject(
                        value = """
                        {
                          "success": true,
                          "data": {
                            "tempC": 22.5,
                            "tempF": 72.5,
                            "conditionCode": "clear",
                            "conditionText": "Clear sky",
                            "humidity": 65,
                            "windSpeedKmh": 12.5,
                            "daypart": "afternoon",
                            "timestamp": "2024-01-15T14:30:00"
                          }
                        }
                        """
                    )]
                )]
            ),
            SwaggerApiResponse(
                responseCode = "400",
                description = "잘못된 위치 좌표"
            )
        ]
    )
    fun getCurrentWeather(
        @Parameter(description = "위도", required = true, example = "37.5665")
        @RequestParam lat: Double,
        @Parameter(description = "경도", required = true, example = "126.9780")
        @RequestParam lon: Double,
        httpRequest: HttpServletRequest
    ): ResponseEntity<ApiResponse<WeatherData>> {
        return try {
            // Validate coordinates
            if (lat < -90 || lat > 90) {
                return ResponseEntity.badRequest().body(
                    ApiResponse.error(
                        code = "INVALID_COORDINATES",
                        message = "위도는 -90에서 90 사이의 값이어야 합니다",
                        path = httpRequest.requestURI
                    )
                )
            }
            
            if (lon < -180 || lon > 180) {
                return ResponseEntity.badRequest().body(
                    ApiResponse.error(
                        code = "INVALID_COORDINATES", 
                        message = "경도는 -180에서 180 사이의 값이어야 합니다",
                        path = httpRequest.requestURI
                    )
                )
            }
            
            logger.info("Fetching weather data for coordinates: lat=$lat, lon=$lon")
            val weatherData = weatherService.getCurrentWeather(lat, lon)
            
            ResponseEntity.ok(ApiResponse.success(weatherData))
            
        } catch (e: Exception) {
            logger.error("Failed to fetch weather data for lat=$lat, lon=$lon", e)
            ResponseEntity.status(500).body(
                ApiResponse.error(
                    code = "WEATHER_SERVICE_ERROR",
                    message = "날씨 정보를 가져오는데 실패했습니다: ${e.message}",
                    path = httpRequest.requestURI
                )
            )
        }
    }
    
    @GetMapping("/context")
    @Operation(
        summary = "날씨 컨텍스트 정보 조회",
        description = "장소 추천에 사용할 날씨 컨텍스트 정보를 조회합니다. (비/추위/더위 여부, 추천 활동 등)"
    )
    @ApiResponses(
        value = [
            SwaggerApiResponse(
                responseCode = "200",
                description = "날씨 컨텍스트 조회 성공",
                content = [Content(
                    mediaType = "application/json",
                    schema = Schema(implementation = WeatherContext::class),
                    examples = [ExampleObject(
                        value = """
                        {
                          "success": true,
                          "data": {
                            "weather": {
                              "tempC": 22.5,
                              "tempF": 72.5,
                              "conditionCode": "clear",
                              "conditionText": "Clear sky",
                              "humidity": 65,
                              "windSpeedKmh": 12.5,
                              "daypart": "afternoon"
                            },
                            "isRainy": false,
                            "isCold": false,
                            "isHot": false,
                            "isComfortable": true,
                            "recommendedActivities": ["outdoor", "walking", "park"]
                          }
                        }
                        """
                    )]
                )]
            )
        ]
    )
    fun getWeatherContext(
        @Parameter(description = "위도", required = true, example = "37.5665")
        @RequestParam lat: Double,
        @Parameter(description = "경도", required = true, example = "126.9780")
        @RequestParam lon: Double,
        httpRequest: HttpServletRequest
    ): ResponseEntity<ApiResponse<WeatherContext>> {
        return try {
            // Validate coordinates
            if (lat < -90 || lat > 90 || lon < -180 || lon > 180) {
                return ResponseEntity.badRequest().body(
                    ApiResponse.error(
                        code = "INVALID_COORDINATES",
                        message = "유효하지 않은 좌표입니다",
                        path = httpRequest.requestURI
                    )
                )
            }
            
            logger.info("Fetching weather context for coordinates: lat=$lat, lon=$lon")
            val weatherContext = weatherService.getWeatherContext(lat, lon)
            
            ResponseEntity.ok(ApiResponse.success(weatherContext))
            
        } catch (e: Exception) {
            logger.error("Failed to fetch weather context for lat=$lat, lon=$lon", e)
            ResponseEntity.status(500).body(
                ApiResponse.error(
                    code = "WEATHER_SERVICE_ERROR",
                    message = "날씨 컨텍스트를 가져오는데 실패했습니다: ${e.message}",
                    path = httpRequest.requestURI
                )
            )
        }
    }
}