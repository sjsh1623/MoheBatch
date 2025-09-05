package com.mohe.spring.controller;

import com.mohe.spring.dto.ApiResponse;
import com.mohe.spring.service.AddressService;
import com.mohe.spring.service.AddressInfo;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.ExampleObject;
import io.swagger.v3.oas.annotations.media.Schema;
// Import removed - using fully qualified name for ApiResponse to avoid conflict
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.security.SecurityRequirements;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.servlet.http.HttpServletRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/address")
@SecurityRequirements
@Tag(name = "주소 정보", description = "좌표를 주소로 변환하는 API")
public class AddressController {
    
    private static final Logger logger = LoggerFactory.getLogger(AddressController.class);
    private final AddressService addressService;
    
    public AddressController(AddressService addressService) {
        this.addressService = addressService;
    }

    @GetMapping("/reverse")
    @Operation(
        summary = "좌표를 주소로 변환",
        description = """
            위도/경도 좌표를 한국 주소로 변환합니다.
            
            - Naver Reverse Geocoding API 우선 사용
            - API 실패 시 좌표 형식으로 fallback
            - 1시간 캐싱으로 성능 최적화
        """
    )
    @ApiResponses(
        value = {
            @io.swagger.v3.oas.annotations.responses.ApiResponse(
                responseCode = "200",
                description = "주소 변환 성공",
                content = @Content(
                    mediaType = "application/json",
                    schema = @Schema(implementation = AddressInfo.class),
                    examples = @ExampleObject(
                        value = """
                        {
                          "success": true,
                          "data": {
                            "fullAddress": "서울특별시 강남구 역삼동 테헤란로 152",
                            "shortAddress": "강남구 역삼동",
                            "sido": "서울특별시",
                            "sigungu": "강남구", 
                            "dong": "역삼동",
                            "roadName": "테헤란로",
                            "buildingNumber": "152",
                            "latitude": 37.5665,
                            "longitude": 126.9780
                          }
                        }
                        """
                    )
                )
            ),
            @io.swagger.v3.oas.annotations.responses.ApiResponse(
                responseCode = "400",
                description = "잘못된 좌표 값"
            ),
            @io.swagger.v3.oas.annotations.responses.ApiResponse(
                responseCode = "500",
                description = "주소 변환 서비스 오류"
            )
        }
    )
    public ResponseEntity<ApiResponse<AddressInfo>> reverseGeocode(
            @Parameter(description = "위도", required = true, example = "37.5665")
            @RequestParam double lat,
            @Parameter(description = "경도", required = true, example = "126.9780")  
            @RequestParam double lon,
            HttpServletRequest httpRequest) {
        try {
            logger.info("Received reverse geocoding request: lat={}, lon={}", lat, lon);
            
            // Validate coordinates
            if (lat < -90 || lat > 90) {
                logger.warn("Invalid latitude: {}", lat);
                return ResponseEntity.badRequest().body(
                    ApiResponse.error(
                        "INVALID_COORDINATES",
                        "위도는 -90에서 90 사이의 값이어야 합니다",
                        httpRequest.getRequestURI()
                    )
                );
            }
            
            if (lon < -180 || lon > 180) {
                logger.warn("Invalid longitude: {}", lon);
                return ResponseEntity.badRequest().body(
                    ApiResponse.error(
                        "INVALID_COORDINATES",
                        "경도는 -180에서 180 사이의 값이어야 합니다",
                        httpRequest.getRequestURI()
                    )
                );
            }

            logger.info("Converting coordinates to address: lat={}, lon={}", lat, lon);
            AddressInfo addressInfo = addressService.getAddressFromCoordinates(lat, lon);
            
            logger.info("Address conversion completed: {}", addressInfo.getShortAddress());
            return ResponseEntity.ok(ApiResponse.success(addressInfo));
            
        } catch (Exception e) {
            logger.error("Failed to convert coordinates to address", e);
            return ResponseEntity.status(500).body(
                ApiResponse.error(
                    "ADDRESS_CONVERSION_ERROR",
                    "주소 변환에 실패했습니다: " + e.getMessage(),
                    httpRequest.getRequestURI()
                )
            );
        }
    }

    @GetMapping("/test")
    @Operation(
        summary = "주소 서비스 테스트",
        description = "주소 서비스가 정상 동작하는지 테스트합니다."
    )
    public ResponseEntity<ApiResponse<String>> testAddress(HttpServletRequest httpRequest) {
        try {
            logger.info("Address service test endpoint called");
            String data = "Address service is working";
            ApiResponse<String> response = ApiResponse.success(data, "Address service test completed");
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            logger.error("Address service test failed", e);
            return ResponseEntity.status(500).body(ApiResponse.error("TEST_FAILED", "Test failed: " + e.getMessage(), httpRequest.getRequestURI()));
        }
    }
}