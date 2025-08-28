package com.mohe.spring.controller

import com.mohe.spring.dto.ApiResponse
import com.mohe.spring.service.*
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
import jakarta.validation.Valid
import org.slf4j.LoggerFactory
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.*

@RestController
@RequestMapping("/api/recommendations")
@SecurityRequirements
@Tag(name = "컨텍스트 기반 추천", description = "날씨, 시간, 위치를 고려한 장소 추천 API")
class ContextualRecommendationController(
    private val contextualRecommendationService: ContextualRecommendationService
) {
    private val logger = LoggerFactory.getLogger(ContextualRecommendationController::class.java)

    @PostMapping("/query")
    @Operation(
        summary = "컨텍스트 기반 장소 추천",
        description = """
            사용자의 자연어 쿼리를 분석하여 현재 날씨, 시간, 위치를 고려한 개인화된 장소를 추천합니다.
            
            Pipeline:
            1. Ollama를 사용한 쿼리 키워드 추출
            2. 현재 위치의 날씨 정보 조회
            3. 시간대 분석 (morning/afternoon/evening/night)
            4. pgvector를 활용한 의미론적 유사도 검색
            5. 날씨/시간/거리 기반 재순위 매기기
            6. 추천 이유 생성
        """
    )
    @ApiResponses(
        value = [
            SwaggerApiResponse(
                responseCode = "200",
                description = "컨텍스트 기반 추천 성공",
                content = [Content(
                    mediaType = "application/json",
                    schema = Schema(implementation = ContextualRecommendationResponse::class),
                    examples = [ExampleObject(
                        value = """
                        {
                          "success": true,
                          "data": {
                            "places": [
                              {
                                "id": 1,
                                "name": "블루보틀 청담점",
                                "category": "카페",
                                "description": "조용하고 세련된 분위기의 스페셜티 커피전문점",
                                "latitude": 37.5200,
                                "longitude": 127.0431,
                                "rating": 4.6,
                                "reviewCount": 1250,
                                "imageUrl": "https://example.com/bluebottle.jpg",
                                "images": ["https://example.com/image1.jpg"],
                                "tags": ["조용한", "커피", "디저트"],
                                "operatingHours": "08:00-21:00",
                                "distanceM": 850,
                                "isOpenNow": true,
                                "score": 0.85,
                                "reasonWhy": "비 오는 날씨에 적합한 실내 공간, 높은 평점 (4.6점)",
                                "weatherSuitability": "비 오는 날에 적합",
                                "timeSuitability": "아침에 좋음"
                              }
                            ],
                            "searchContext": {
                              "query": "비 와서 따뜻한 카페 가고싶어",
                              "extractedKeywords": ["카페", "따뜻한", "실내", "커피"],
                              "weather": {
                                "tempC": 15.2,
                                "tempF": 59.4,
                                "conditionCode": "rain",
                                "conditionText": "Light rain",
                                "humidity": 78,
                                "windSpeedKmh": 12.5,
                                "daypart": "afternoon"
                              },
                              "daypart": "afternoon",
                              "localTime": "2024-01-15T14:30:00",
                              "locationDescription": "위도 37.5200, 경도 127.0431",
                              "recommendation": "비 오는 날씨와 오후 시간를 고려하여 추천드립니다."
                            },
                            "totalResults": 25
                          }
                        }
                        """
                    )]
                )]
            ),
            SwaggerApiResponse(
                responseCode = "400",
                description = "잘못된 요청 파라미터"
            ),
            SwaggerApiResponse(
                responseCode = "500",
                description = "추천 시스템 오류"
            )
        ]
    )
    fun getContextualRecommendations(
        @Parameter(description = "컨텍스트 기반 추천 요청", required = true)
        @Valid @RequestBody request: ContextualRecommendationRequest,
        httpRequest: HttpServletRequest
    ): ResponseEntity<ApiResponse<ContextualRecommendationResponse>> {
        return try {
            // Validate input parameters
            if (request.query.isBlank()) {
                return ResponseEntity.badRequest().body(
                    ApiResponse.error(
                        code = "INVALID_QUERY",
                        message = "검색 쿼리는 비어있을 수 없습니다",
                        path = httpRequest.requestURI
                    )
                )
            }
            
            if (request.lat < -90 || request.lat > 90) {
                return ResponseEntity.badRequest().body(
                    ApiResponse.error(
                        code = "INVALID_COORDINATES",
                        message = "위도는 -90에서 90 사이의 값이어야 합니다",
                        path = httpRequest.requestURI
                    )
                )
            }
            
            if (request.lon < -180 || request.lon > 180) {
                return ResponseEntity.badRequest().body(
                    ApiResponse.error(
                        code = "INVALID_COORDINATES",
                        message = "경도는 -180에서 180 사이의 값이어야 합니다",
                        path = httpRequest.requestURI
                    )
                )
            }
            
            if (request.limit < 1 || request.limit > 50) {
                return ResponseEntity.badRequest().body(
                    ApiResponse.error(
                        code = "INVALID_LIMIT",
                        message = "limit은 1에서 50 사이의 값이어야 합니다",
                        path = httpRequest.requestURI
                    )
                )
            }
            
            logger.info("Processing contextual recommendation request: query='${request.query}', lat=${request.lat}, lon=${request.lon}")
            
            val response = contextualRecommendationService.getContextualRecommendations(request)
            
            logger.info("Contextual recommendation completed: found ${response.places.size} places for query '${request.query}'")
            
            ResponseEntity.ok(ApiResponse.success(response))
            
        } catch (e: Exception) {
            logger.error("Failed to get contextual recommendations", e)
            ResponseEntity.status(500).body(
                ApiResponse.error(
                    code = "RECOMMENDATION_ERROR",
                    message = "컨텍스트 기반 추천에 실패했습니다: ${e.message}",
                    path = httpRequest.requestURI
                )
            )
        }
    }

    @PostMapping("/search")
    @Operation(
        summary = "키워드 기반 장소 검색",
        description = "정확한 키워드와 위치 기반으로 장소를 검색합니다. 벡터 유사도와 지리적 거리를 활용합니다."
    )
    @ApiResponses(
        value = [
            SwaggerApiResponse(
                responseCode = "200",
                description = "키워드 검색 성공",
                content = [Content(
                    mediaType = "application/json",
                    examples = [ExampleObject(
                        value = """
                        {
                          "success": true,
                          "data": {
                            "places": [
                              {
                                "id": 1,
                                "name": "스타벅스 강남점",
                                "category": "카페",
                                "latitude": 37.5665,
                                "longitude": 126.9780,
                                "rating": 4.2,
                                "reviewCount": 850,
                                "distanceM": 500,
                                "isOpenNow": true,
                                "score": 0.75
                              }
                            ],
                            "totalResults": 15
                          }
                        }
                        """
                    )]
                )]
            )
        ]
    )
    fun searchPlaces(
        @Parameter(description = "키워드 검색 요청", required = true)
        @Valid @RequestBody request: PlaceSearchRequest,
        httpRequest: HttpServletRequest
    ): ResponseEntity<ApiResponse<PlaceSearchResponse>> {
        return try {
            // Validate input
            if (request.keywords.isEmpty()) {
                return ResponseEntity.badRequest().body(
                    ApiResponse.error(
                        code = "INVALID_KEYWORDS",
                        message = "최소 하나의 키워드가 필요합니다",
                        path = httpRequest.requestURI
                    )
                )
            }
            
            if (request.lat < -90 || request.lat > 90 || request.lon < -180 || request.lon > 180) {
                return ResponseEntity.badRequest().body(
                    ApiResponse.error(
                        code = "INVALID_COORDINATES",
                        message = "유효하지 않은 좌표입니다",
                        path = httpRequest.requestURI
                    )
                )
            }
            
            logger.info("Processing place search request: keywords=${request.keywords}, lat=${request.lat}, lon=${request.lon}")
            
            // Convert to contextual recommendation request
            val contextualRequest = ContextualRecommendationRequest(
                query = request.keywords.joinToString(" "),
                lat = request.lat,
                lon = request.lon,
                limit = request.limit,
                maxDistanceKm = 10.0 // Default max distance for search
            )
            
            val contextualResponse = contextualRecommendationService.getContextualRecommendations(contextualRequest)
            
            // Convert to simplified search response
            val searchResponse = PlaceSearchResponse(
                places = contextualResponse.places.map { contextualPlace ->
                    SimplePlace(
                        id = contextualPlace.id,
                        name = contextualPlace.name,
                        category = contextualPlace.category,
                        latitude = contextualPlace.latitude,
                        longitude = contextualPlace.longitude,
                        rating = contextualPlace.rating,
                        reviewCount = contextualPlace.reviewCount,
                        imageUrl = contextualPlace.imageUrl,
                        images = contextualPlace.images,
                        tags = contextualPlace.tags,
                        distanceM = contextualPlace.distanceM,
                        isOpenNow = contextualPlace.isOpenNow,
                        score = contextualPlace.score
                    )
                },
                totalResults = contextualResponse.totalResults
            )
            
            ResponseEntity.ok(ApiResponse.success(searchResponse))
            
        } catch (e: Exception) {
            logger.error("Failed to search places", e)
            ResponseEntity.status(500).body(
                ApiResponse.error(
                    code = "SEARCH_ERROR",
                    message = "장소 검색에 실패했습니다: ${e.message}",
                    path = httpRequest.requestURI
                )
            )
        }
    }
}

/**
 * Simple place search request
 */
data class PlaceSearchRequest(
    val keywords: List<String>,
    val lat: Double,
    val lon: Double,
    val limit: Int = 10
)

/**
 * Simplified place search response
 */
data class PlaceSearchResponse(
    val places: List<SimplePlace>,
    val totalResults: Int
)

/**
 * Simplified place for search results
 */
data class SimplePlace(
    val id: Long,
    val name: String,
    val category: String?,
    val latitude: Double?,
    val longitude: Double?,
    val rating: Double,
    val reviewCount: Int,
    val imageUrl: String?,
    val images: List<String>,
    val tags: List<String>,
    val distanceM: Int,
    val isOpenNow: Boolean,
    val score: Double
)