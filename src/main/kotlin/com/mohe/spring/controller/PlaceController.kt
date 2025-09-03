package com.mohe.spring.controller

import com.mohe.spring.dto.*
import com.mohe.spring.service.PlaceService
import io.swagger.v3.oas.annotations.Operation
import io.swagger.v3.oas.annotations.Parameter
import io.swagger.v3.oas.annotations.media.Content
import io.swagger.v3.oas.annotations.media.ExampleObject
import io.swagger.v3.oas.annotations.media.Schema
import io.swagger.v3.oas.annotations.responses.ApiResponse as SwaggerApiResponse
import io.swagger.v3.oas.annotations.responses.ApiResponses
import io.swagger.v3.oas.annotations.security.SecurityRequirement
import io.swagger.v3.oas.annotations.tags.Tag
import jakarta.servlet.http.HttpServletRequest
import org.springframework.http.ResponseEntity
import org.springframework.security.access.prepost.PreAuthorize
import org.springframework.web.bind.annotation.*

@RestController
@RequestMapping("/api/places")
@Tag(name = "장소 관리", description = "장소 추천, 검색, 상세 정보 API")
class PlaceController(
    private val placeService: PlaceService
) {
    
    @GetMapping("/recommendations")
    @PreAuthorize("hasRole('USER')")
    @SecurityRequirement(name = "bearerAuth")
    @Operation(
        summary = "개인화된 장소 추천",
        description = "사용자의 MBTI와 선호도를 기반으로 개인화된 장소를 추천합니다."
    )
    @ApiResponses(
        value = [
            SwaggerApiResponse(
                responseCode = "200",
                description = "추천 장소 조회 성공",
                content = [Content(
                    mediaType = "application/json",
                    schema = Schema(implementation = PlaceRecommendationsResponse::class)
                )]
            )
        ]
    )
    fun getRecommendations(
        httpRequest: HttpServletRequest
    ): ResponseEntity<ApiResponse<PlaceRecommendationsResponse>> {
        return try {
            val response = placeService.getRecommendations()
            ResponseEntity.ok(ApiResponse.success(response))
        } catch (e: Exception) {
            ResponseEntity.badRequest().body(
                ApiResponse.error(
                    code = ErrorCode.INTERNAL_SERVER_ERROR,
                    message = e.message ?: "추천 장소 조회에 실패했습니다",
                    path = httpRequest.requestURI
                )
            )
        }
    }
    
    @GetMapping
    @Operation(
        summary = "장소 목록 조회",
        description = "페이지네이션과 필터링을 지원하는 장소 목록을 조회합니다."
    )
    @ApiResponses(
        value = [
            SwaggerApiResponse(
                responseCode = "200",
                description = "장소 목록 조회 성공",
                content = [Content(
                    mediaType = "application/json",
                    schema = Schema(implementation = PlaceListResponse::class)
                )]
            )
        ]
    )
    fun getPlaces(
        @Parameter(description = "페이지 번호 (1부터 시작)", example = "1")
        @RequestParam(defaultValue = "1") page: Int,
        @Parameter(description = "페이지당 아이템 개수", example = "20")
        @RequestParam(defaultValue = "20") limit: Int,
        @Parameter(description = "카테고리 필터", example = "cafe")
        @RequestParam(required = false) category: String?,
        @Parameter(description = "정렬 방식 (rating, popularity)", example = "rating")
        @RequestParam(defaultValue = "rating") sort: String,
        httpRequest: HttpServletRequest
    ): ResponseEntity<ApiResponse<PlaceListResponse>> {
        return try {
            val response = placeService.getPlaces(page, limit, category, sort)
            ResponseEntity.ok(ApiResponse.success(response))
        } catch (e: Exception) {
            ResponseEntity.badRequest().body(
                ApiResponse.error(
                    code = ErrorCode.INTERNAL_SERVER_ERROR,
                    message = e.message ?: "장소 목록 조회에 실패했습니다",
                    path = httpRequest.requestURI
                )
            )
        }
    }
    
    @GetMapping("/{id}")
    @Operation(
        summary = "장소 상세 정보 조회",
        description = "지정된 장소의 상세 정보를 조회하고 최근 조회 이력에 추가합니다."
    )
    @ApiResponses(
        value = [
            SwaggerApiResponse(
                responseCode = "200",
                description = "장소 상세 정보 조회 성공",
                content = [Content(
                    mediaType = "application/json",
                    schema = Schema(implementation = PlaceDetailResponse::class)
                )]
            ),
            SwaggerApiResponse(
                responseCode = "404",
                description = "장소를 찾을 수 없음"
            )
        ]
    )
    fun getPlaceDetail(
        @Parameter(description = "장소 ID", required = true, example = "1")
        @PathVariable id: String,
        httpRequest: HttpServletRequest
    ): ResponseEntity<ApiResponse<PlaceDetailResponse>> {
        return try {
            val response = placeService.getPlaceDetail(id)
            ResponseEntity.ok(ApiResponse.success(response))
        } catch (e: Exception) {
            val errorCode = when {
                e.message?.contains("찾을 수 없습니다") == true -> ErrorCode.RESOURCE_NOT_FOUND
                else -> ErrorCode.INTERNAL_SERVER_ERROR
            }
            ResponseEntity.badRequest().body(
                ApiResponse.error(
                    code = errorCode,
                    message = e.message ?: "장소 상세 조회에 실패했습니다",
                    path = httpRequest.requestURI
                )
            )
        }
    }
    
    @GetMapping("/search")
    @Operation(
        summary = "장소 검색",
        description = "컴텍스트 기반 장소 검색 (날씨, 시간, 위치 고려)"
    )
    @ApiResponses(
        value = [
            SwaggerApiResponse(
                responseCode = "200",
                description = "장소 검색 성공",
                content = [Content(
                    mediaType = "application/json",
                    schema = Schema(implementation = PlaceSearchResponse::class),
                    examples = [ExampleObject(
                        value = """
                        {
                          "success": true,
                          "data": {
                            "searchResults": [
                              {
                                "id": "1",
                                "name": "카페 무브먼트랩",
                                "hours": "09:00 ~ 19:00",
                                "location": "서울 성수동",
                                "rating": 4.7,
                                "carTime": "5분",
                                "busTime": "10분",
                                "tags": ["#조용한", "#카페", "#시원한"],
                                "image": "https://example.com/place.jpg",
                                "isBookmarked": false,
                                "weatherTag": {
                                  "text": "더운 날씨에 가기 좋은 카페",
                                  "color": "red",
                                  "icon": "thermometer"
                                }
                              }
                            ],
                            "searchContext": {
                              "weather": "더운 날씨",
                              "time": "오후 2시",
                              "recommendation": "지금은 멀지 않고 실내 장소들을 추천드릴께요."
                            }
                          }
                        }
                        """
                    )]
                )]
            )
        ]
    )
    fun searchPlaces(
        @Parameter(description = "검색 쿠리", required = true, example = "카페")
        @RequestParam q: String,
        @Parameter(description = "위치 필터", example = "성수동")
        @RequestParam(required = false) location: String?,
        @Parameter(description = "날씨 컴텍스트 (hot, cold)", example = "hot")
        @RequestParam(required = false) weather: String?,
        @Parameter(description = "시간 컴텍스트 (morning, afternoon, evening)", example = "afternoon")
        @RequestParam(required = false) time: String?,
        httpRequest: HttpServletRequest
    ): ResponseEntity<ApiResponse<PlaceSearchResponse>> {
        return try {
            val response = placeService.searchPlaces(q, location, weather, time)
            ResponseEntity.ok(ApiResponse.success(response))
        } catch (e: Exception) {
            ResponseEntity.badRequest().body(
                ApiResponse.error(
                    code = ErrorCode.INTERNAL_SERVER_ERROR,
                    message = e.message ?: "장소 검색에 실패했습니다",
                    path = httpRequest.requestURI
                )
            )
        }
    }

    @GetMapping("/debug")
    @Operation(
        summary = "디버그 - 장소 데이터 확인",
        description = "데이터베이스의 장소 데이터를 확인하기 위한 디버그 엔드포인트"
    )
    @ApiResponses(
        value = [
            SwaggerApiResponse(
                responseCode = "200", 
                description = "디버그 정보 조회 성공"
            )
        ]
    )
    fun debugPlaces(
        httpRequest: HttpServletRequest
    ): ResponseEntity<ApiResponse<Map<String, Any>>> {
        return try {
            val totalPlaces = placeService.getDebugInfo()
            ResponseEntity.ok(ApiResponse.success(totalPlaces))
        } catch (e: Exception) {
            ResponseEntity.badRequest().body(
                ApiResponse.error(
                    code = ErrorCode.INTERNAL_SERVER_ERROR,
                    message = e.message ?: "디버그 정보 조회에 실패했습니다",
                    path = httpRequest.requestURI
                )
            )
        }
    }

    @GetMapping("/popular")
    @Operation(
        summary = "인기 장소 목록 조회",
        description = "사용자 위치를 기반으로 10km 이내의 인기 장소를 북마크 순으로 조회합니다. 게스트와 로그인 사용자 모두 접근 가능합니다."
    )
    @ApiResponses(
        value = [
            SwaggerApiResponse(
                responseCode = "200",
                description = "인기 장소 목록 조회 성공",
                content = [Content(
                    mediaType = "application/json",
                    schema = Schema(implementation = PlaceListResponse::class)
                )]
            )
        ]
    )
    fun getPopularPlaces(
        @Parameter(description = "사용자 위도", required = true, example = "37.5665")
        @RequestParam latitude: Double,
        @Parameter(description = "사용자 경도", required = true, example = "126.9780")
        @RequestParam longitude: Double,
        httpRequest: HttpServletRequest
    ): ResponseEntity<ApiResponse<PlaceListResponse>> {
        return try {
            val response = placeService.getPopularPlaces(latitude, longitude)
            ResponseEntity.ok(ApiResponse.success(response))
        } catch (e: Exception) {
            ResponseEntity.badRequest().body(
                ApiResponse.error(
                    code = ErrorCode.INTERNAL_SERVER_ERROR,
                    message = e.message ?: "인기 장소 목록 조회에 실패했습니다",
                    path = httpRequest.requestURI
                )
            )
        }
    }

    @GetMapping("/current-time")
    @Operation(
        summary = "지금 이 시간의 장소 추천",
        description = "현재 시간과 날씨를 기반으로 적합한 장소를 추천합니다. 로그인 여부와 관계없이 모든 사용자가 접근 가능합니다."
    )
    @ApiResponses(
        value = [
            SwaggerApiResponse(
                responseCode = "200",
                description = "현재 시간대 추천 장소 조회 성공",
                content = [Content(
                    mediaType = "application/json",
                    schema = Schema(implementation = CurrentTimeRecommendationsResponse::class)
                )]
            )
        ]
    )
    fun getCurrentTimePlaces(
        @Parameter(description = "사용자 위도", example = "37.5665")
        @RequestParam(required = false) latitude: Double?,
        @Parameter(description = "사용자 경도", example = "126.9780")
        @RequestParam(required = false) longitude: Double?,
        @Parameter(description = "페이지 크기", example = "10")
        @RequestParam(defaultValue = "10") limit: Int,
        httpRequest: HttpServletRequest
    ): ResponseEntity<ApiResponse<CurrentTimeRecommendationsResponse>> {
        return try {
            val safeLimit = if (limit < 1) 10 else if (limit > 50) 50 else limit
            val response = placeService.getCurrentTimePlaces(latitude, longitude, safeLimit)
            ResponseEntity.ok(ApiResponse.success(response))
        } catch (e: Exception) {
            ResponseEntity.badRequest().body(
                ApiResponse.error(
                    code = ErrorCode.INTERNAL_SERVER_ERROR,
                    message = e.message ?: "현재 시간 추천 장소 조회에 실패했습니다",
                    path = httpRequest.requestURI
                )
            )
        }
    }

    @Operation(
        summary = "Get general places list",
        description = "Get a general list of places with pagination and sorting options"
    )
    @ApiResponses(
        value = [
            SwaggerApiResponse(responseCode = "200", description = "Places retrieved successfully"),
            SwaggerApiResponse(responseCode = "400", description = "Invalid parameters")
        ]
    )
    @GetMapping("/list")
    fun getPlacesList(
        @Parameter(description = "페이지 번호 (0부터 시작)", example = "0")
        @RequestParam(defaultValue = "0") page: Int,
        @Parameter(description = "페이지 크기", example = "10") 
        @RequestParam(defaultValue = "10") limit: Int,
        @Parameter(description = "정렬 기준 (popularity, rating, recent)", example = "popularity")
        @RequestParam(defaultValue = "popularity") sort: String,
        httpRequest: HttpServletRequest
    ): ResponseEntity<ApiResponse<PlaceListResponse>> {
        return try {
            val safePage = if (page < 0) 0 else page
            val safeLimit = if (limit < 1) 10 else if (limit > 100) 100 else limit
            
            val response = placeService.getPlacesList(safePage, safeLimit, sort)
            ResponseEntity.ok(ApiResponse.success(response))
        } catch (e: Exception) {
            ResponseEntity.badRequest().body(
                ApiResponse.error(
                    code = ErrorCode.INTERNAL_SERVER_ERROR,
                    message = e.message ?: "장소 목록 조회에 실패했습니다",
                    path = httpRequest.requestURI
                )
            )
        }
    }
}