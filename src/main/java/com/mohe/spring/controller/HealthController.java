package com.mohe.spring.controller;

import com.mohe.spring.dto.ApiResponse;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.ExampleObject;
// Import removed - using fully qualified name for ApiResponse to avoid conflict
import io.swagger.v3.oas.annotations.security.SecurityRequirements;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.time.OffsetDateTime;
import java.util.Map;

@RestController
@Tag(name = "시스템", description = "시스템 상태 및 헬스체크 API")
public class HealthController {
    
    @GetMapping("/health")
    @SecurityRequirements
    @Operation(
        summary = "애플리케이션 상태 확인",
        description = "애플리케이션의 현재 상태와 버전 정보를 확인합니다."
    )
    @io.swagger.v3.oas.annotations.responses.ApiResponse(
        responseCode = "200",
        description = "애플리케이션 상태 조회 성공",
        content = @Content(
            mediaType = "application/json",
            examples = @ExampleObject(
                value = """
                {
                  "success": true,
                  "data": {
                    "status": "UP",
                    "timestamp": "2024-01-01T12:00:00Z",
                    "service": "Mohe Spring Application",
                    "version": "1.0.0"
                  }
                }
                """
            )
        )
    )
    public ApiResponse<Map<String, Object>> health() {
        Map<String, Object> healthData = Map.of(
            "status", "UP",
            "timestamp", OffsetDateTime.now(),
            "service", "Mohe Spring Application",
            "version", "1.0.0"
        );
        
        return ApiResponse.success(healthData);
    }
}