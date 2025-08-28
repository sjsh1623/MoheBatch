package com.mohe.spring.controller

import com.mohe.spring.service.ContextualRecommendationService
import com.mohe.spring.service.ContextualRecommendationRequest
import com.mohe.spring.service.ContextualRecommendationResponse
import com.mohe.spring.service.ContextualPlace
import com.mohe.spring.service.SearchContext
import com.mohe.spring.service.WeatherData
import com.fasterxml.jackson.databind.ObjectMapper
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.DisplayName
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest
import org.springframework.boot.test.mock.mockito.MockBean
import org.springframework.http.MediaType
import org.springframework.test.web.servlet.MockMvc
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*
import org.springframework.test.web.servlet.result.MockMvcResultMatchers.*
import org.mockito.kotlin.whenever
import org.mockito.kotlin.any
import java.time.LocalDateTime

@WebMvcTest(ContextualRecommendationController::class)
@DisplayName("ContextualRecommendationController Tests")
class ContextualRecommendationControllerTest {

    @Autowired
    private lateinit var mockMvc: MockMvc
    
    @Autowired
    private lateinit var objectMapper: ObjectMapper

    @MockBean
    private lateinit var contextualRecommendationService: ContextualRecommendationService

    @Test
    @DisplayName("Should get contextual recommendations successfully")
    fun shouldGetContextualRecommendationsSuccessfully() {
        // Given
        val request = ContextualRecommendationRequest(
            query = "비 와서 따뜻한 카페 가고싶어",
            lat = 37.5665,
            lon = 126.9780,
            limit = 10,
            maxDistanceKm = 5.0
        )

        val mockResponse = ContextualRecommendationResponse(
            places = listOf(
                ContextualPlace(
                    id = 1L,
                    name = "블루보틀 청담점",
                    category = "카페",
                    description = "조용하고 세련된 분위기의 스페셜티 커피전문점",
                    latitude = 37.5200,
                    longitude = 127.0431,
                    rating = 4.6,
                    reviewCount = 1250,
                    imageUrl = "https://example.com/bluebottle.jpg",
                    images = listOf("https://example.com/image1.jpg"),
                    tags = listOf("조용한", "커피", "디저트"),
                    operatingHours = "08:00-21:00",
                    distanceM = 850,
                    isOpenNow = true,
                    score = 0.85,
                    reasonWhy = "비 오는 날씨에 적합한 실내 공간, 높은 평점 (4.6점)",
                    weatherSuitability = "비 오는 날에 적합",
                    timeSuitability = "아침에 좋음"
                )
            ),
            searchContext = SearchContext(
                query = "비 와서 따뜻한 카페 가고싶어",
                extractedKeywords = listOf("카페", "따뜻한", "실내", "커피"),
                weather = WeatherData(
                    tempC = 15.2,
                    tempF = 59.4,
                    conditionCode = "rain",
                    conditionText = "Light rain",
                    humidity = 78,
                    windSpeedKmh = 12.5,
                    daypart = "afternoon",
                    timestamp = LocalDateTime.now()
                ),
                daypart = "afternoon",
                localTime = LocalDateTime.now(),
                locationDescription = "위도 37.5200, 경도 127.0431",
                recommendation = "비 오는 날씨와 오후 시간을 고려하여 추천드립니다."
            ),
            totalResults = 25
        )

        whenever(contextualRecommendationService.getContextualRecommendations(any()))
            .thenReturn(mockResponse)

        // When & Then
        mockMvc.perform(
            post("/api/recommendations/query")
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(request))
        )
            .andExpect(status().isOk)
            .andExpect(jsonPath("$.success").value(true))
            .andExpect(jsonPath("$.data.places").isArray)
            .andExpect(jsonPath("$.data.places[0].name").value("블루보틀 청담점"))
            .andExpect(jsonPath("$.data.places[0].category").value("카페"))
            .andExpect(jsonPath("$.data.places[0].rating").value(4.6))
            .andExpect(jsonPath("$.data.places[0].reasonWhy").value("비 오는 날씨에 적합한 실내 공간, 높은 평점 (4.6점)"))
            .andExpect(jsonPath("$.data.searchContext.query").value("비 와서 따뜻한 카페 가고싶어"))
            .andExpect(jsonPath("$.data.searchContext.extractedKeywords").isArray)
            .andExpect(jsonPath("$.data.searchContext.weather.conditionCode").value("rain"))
            .andExpect(jsonPath("$.data.totalResults").value(25))
    }

    @Test
    @DisplayName("Should validate empty query")
    fun shouldValidateEmptyQuery() {
        // Given
        val request = ContextualRecommendationRequest(
            query = "",
            lat = 37.5665,
            lon = 126.9780,
            limit = 10
        )

        // When & Then
        mockMvc.perform(
            post("/api/recommendations/query")
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(request))
        )
            .andExpect(status().isBadRequest)
            .andExpect(jsonPath("$.success").value(false))
            .andExpect(jsonPath("$.code").value("INVALID_QUERY"))
            .andExpect(jsonPath("$.message").value("검색 쿼리는 비어있을 수 없습니다"))
    }

    @Test
    @DisplayName("Should validate invalid latitude")
    fun shouldValidateInvalidLatitude() {
        // Given
        val request = ContextualRecommendationRequest(
            query = "좋은 카페",
            lat = 91.0, // Invalid latitude
            lon = 126.9780,
            limit = 10
        )

        // When & Then
        mockMvc.perform(
            post("/api/recommendations/query")
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(request))
        )
            .andExpect(status().isBadRequest)
            .andExpect(jsonPath("$.success").value(false))
            .andExpect(jsonPath("$.code").value("INVALID_COORDINATES"))
            .andExpect(jsonPath("$.message").value("위도는 -90에서 90 사이의 값이어야 합니다"))
    }

    @Test
    @DisplayName("Should validate invalid longitude")
    fun shouldValidateInvalidLongitude() {
        // Given
        val request = ContextualRecommendationRequest(
            query = "좋은 카페",
            lat = 37.5665,
            lon = 181.0, // Invalid longitude
            limit = 10
        )

        // When & Then
        mockMvc.perform(
            post("/api/recommendations/query")
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(request))
        )
            .andExpect(status().isBadRequest)
            .andExpect(jsonPath("$.success").value(false))
            .andExpect(jsonPath("$.code").value("INVALID_COORDINATES"))
            .andExpect(jsonPath("$.message").value("경도는 -180에서 180 사이의 값이어야 합니다"))
    }

    @Test
    @DisplayName("Should validate invalid limit")
    fun shouldValidateInvalidLimit() {
        // Given
        val request = ContextualRecommendationRequest(
            query = "좋은 카페",
            lat = 37.5665,
            lon = 126.9780,
            limit = 100 // Exceeds maximum
        )

        // When & Then
        mockMvc.perform(
            post("/api/recommendations/query")
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(request))
        )
            .andExpect(status().isBadRequest)
            .andExpect(jsonPath("$.success").value(false))
            .andExpect(jsonPath("$.code").value("INVALID_LIMIT"))
            .andExpect(jsonPath("$.message").value("limit은 1에서 50 사이의 값이어야 합니다"))
    }

    @Test
    @DisplayName("Should search places successfully")
    fun shouldSearchPlacesSuccessfully() {
        // Given
        val searchRequest = PlaceSearchRequest(
            keywords = listOf("카페", "조용한"),
            lat = 37.5665,
            lon = 126.9780,
            limit = 10
        )

        val mockContextualResponse = ContextualRecommendationResponse(
            places = listOf(
                ContextualPlace(
                    id = 1L,
                    name = "스타벅스 강남점",
                    category = "카페",
                    description = null,
                    latitude = 37.5665,
                    longitude = 126.9780,
                    rating = 4.2,
                    reviewCount = 850,
                    imageUrl = null,
                    images = emptyList(),
                    tags = emptyList(),
                    operatingHours = null,
                    distanceM = 500,
                    isOpenNow = true,
                    score = 0.75,
                    reasonWhy = null,
                    weatherSuitability = null,
                    timeSuitability = null
                )
            ),
            searchContext = SearchContext(
                query = "카페 조용한",
                extractedKeywords = listOf("카페", "조용한"),
                weather = WeatherData(
                    tempC = 22.0,
                    tempF = 71.6,
                    conditionCode = "clear",
                    conditionText = "Clear",
                    humidity = 60,
                    windSpeedKmh = 10.0,
                    daypart = "afternoon"
                ),
                daypart = "afternoon",
                localTime = LocalDateTime.now(),
                locationDescription = "서울시 강남구",
                recommendation = "현재 날씨와 시간을 고려한 추천입니다."
            ),
            totalResults = 15
        )

        whenever(contextualRecommendationService.getContextualRecommendations(any()))
            .thenReturn(mockContextualResponse)

        // When & Then
        mockMvc.perform(
            post("/api/recommendations/search")
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(searchRequest))
        )
            .andExpect(status().isOk)
            .andExpect(jsonPath("$.success").value(true))
            .andExpected(jsonPath("$.data.places").isArray)
            .andExpect(jsonPath("$.data.places[0].name").value("스타벅스 강남점"))
            .andExpect(jsonPath("$.data.places[0].category").value("카페"))
            .andExpect(jsonPath("$.data.places[0].rating").value(4.2))
            .andExpect(jsonPath("$.data.places[0].distanceM").value(500))
            .andExpect(jsonPath("$.data.totalResults").value(15))
    }

    @Test
    @DisplayName("Should handle service errors gracefully")
    fun shouldHandleServiceErrorsGracefully() {
        // Given
        val request = ContextualRecommendationRequest(
            query = "좋은 카페",
            lat = 37.5665,
            lon = 126.9780,
            limit = 10
        )

        whenever(contextualRecommendationService.getContextualRecommendations(any()))
            .thenThrow(RuntimeException("Service error"))

        // When & Then
        mockMvc.perform(
            post("/api/recommendations/query")
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(request))
        )
            .andExpect(status().isInternalServerError)
            .andExpect(jsonPath("$.success").value(false))
            .andExpect(jsonPath("$.code").value("RECOMMENDATION_ERROR"))
            .andExpect(jsonPath("$.message").contains("컨텍스트 기반 추천에 실패했습니다"))
    }
}