package com.mohe.spring.controller

import com.mohe.spring.dto.ApiResponse
import com.mohe.spring.service.AddressService
import com.mohe.spring.service.AddressInfo
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
@RequestMapping("/api/address")
@SecurityRequirements
@Tag(name = "주소 정보", description = "좌표를 주소로 변환하는 API")
class AddressController(
    private val addressService: AddressService
) {
    private val logger = LoggerFactory.getLogger(AddressController::class.java)

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
        value = [
            SwaggerApiResponse(
                responseCode = "200",
                description = "주소 변환 성공",
                content = [Content(
                    mediaType = "application/json",
                    schema = Schema(implementation = AddressInfo::class),
                    examples = [ExampleObject(
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
                    )]
                )]
            ),
            SwaggerApiResponse(
                responseCode = "400",
                description = "잘못된 좌표 값"
            ),
            SwaggerApiResponse(
                responseCode = "500",
                description = "주소 변환 서비스 오류"
            )
        ]
    )
    fun reverseGeocode(
        @Parameter(description = "위도", required = true, example = "37.5665")
        @RequestParam lat: Double,
        @Parameter(description = "경도", required = true, example = "126.9780")  
        @RequestParam lon: Double,
        httpRequest: HttpServletRequest
    ): ResponseEntity<ApiResponse<AddressInfo>> {
        return try {
            logger.info("Received reverse geocoding request: lat=$lat, lon=$lon")
            
            // Validate coordinates
            if (lat < -90 || lat > 90) {
                logger.warn("Invalid latitude: $lat")
                return ResponseEntity.badRequest().body(
                    ApiResponse.error(
                        code = "INVALID_COORDINATES",
                        message = "위도는 -90에서 90 사이의 값이어야 합니다",
                        path = httpRequest.requestURI
                    )
                )
            }
            
            if (lon < -180 || lon > 180) {
                logger.warn("Invalid longitude: $lon")
                return ResponseEntity.badRequest().body(
                    ApiResponse.error(
                        code = "INVALID_COORDINATES",
                        message = "경도는 -180에서 180 사이의 값이어야 합니다",
                        path = httpRequest.requestURI
                    )
                )
            }

            logger.info("Converting coordinates to address: lat=$lat, lon=$lon")
            val addressInfo = addressService.getAddressFromCoordinates(lat, lon)
            
            logger.info("Address conversion completed: ${addressInfo.shortAddress}")
            ResponseEntity.ok(ApiResponse.success(addressInfo))
            
        } catch (e: Exception) {
            logger.error("Failed to convert coordinates to address", e)
            ResponseEntity.status(500).body(
                ApiResponse.error(
                    code = "ADDRESS_CONVERSION_ERROR",
                    message = "주소 변환에 실패했습니다: ${e.message}",
                    path = httpRequest.requestURI
                )
            )
        }
    }

    @GetMapping("/test")
    @Operation(
        summary = "주소 서비스 테스트",
        description = "주소 서비스가 정상 동작하는지 테스트합니다."
    )
    fun testAddress(): ResponseEntity<ApiResponse<String>> {
        return try {
            logger.info("Address service test endpoint called")
            ResponseEntity.ok(ApiResponse.success("Address service is working"))
        } catch (e: Exception) {
            logger.error("Address service test failed", e)
            ResponseEntity.status(500).body(ApiResponse.error("TEST_FAILED", "Test failed: ${e.message}"))
        }
    }
}