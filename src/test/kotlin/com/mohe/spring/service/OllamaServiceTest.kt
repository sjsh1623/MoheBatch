package com.mohe.spring.service

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.DisplayName
import org.mockito.Mock
import org.mockito.MockitoAnnotations
import org.mockito.kotlin.whenever
import org.mockito.kotlin.any
import org.springframework.web.reactive.function.client.WebClient
import org.springframework.web.reactive.function.client.WebClient.RequestBodyUriSpec
import org.springframework.web.reactive.function.client.WebClient.RequestHeadersSpec
import org.springframework.web.reactive.function.client.WebClient.ResponseSpec
import reactor.core.publisher.Mono
import org.assertj.core.api.Assertions.*
import java.time.Duration

@DisplayName("OllamaService Tests")
class OllamaServiceTest {

    @Mock
    private lateinit var mockWebClient: WebClient
    
    @Mock
    private lateinit var mockRequestBodyUriSpec: RequestBodyUriSpec
    
    @Mock
    private lateinit var mockRequestHeadersSpec: RequestHeadersSpec<*>
    
    @Mock
    private lateinit var mockResponseSpec: ResponseSpec

    private lateinit var ollamaService: OllamaService

    private val ollamaHost = "http://localhost:11434"
    private val ollamaModel = "llama2"
    private val ollamaTimeout = 30

    @BeforeEach
    fun setup() {
        MockitoAnnotations.openMocks(this)
        ollamaService = OllamaService(mockWebClient, ollamaHost, ollamaModel, ollamaTimeout)
    }

    @Test
    @DisplayName("Should extract keywords from query successfully")
    fun shouldExtractKeywordsFromQuerySuccessfully() {
        // Given
        val query = "비 와서 따뜻한 카페 가고싶어"
        val weatherContext = "비 오는 날씨"
        val timeContext = "오후"
        
        val mockResponse = mapOf("response" to "카페, 따뜻한, 실내, 커피, 디저트")
        
        whenever(mockWebClient.post()).thenReturn(mockRequestBodyUriSpec)
        whenever(mockRequestBodyUriSpec.uri(any<String>())).thenReturn(mockRequestBodyUriSpec)
        whenever(mockRequestBodyUriSpec.contentType(any())).thenReturn(mockRequestBodyUriSpec)
        whenever(mockRequestBodyUriSpec.bodyValue(any())).thenReturn(mockRequestHeadersSpec)
        whenever(mockRequestHeadersSpec.retrieve()).thenReturn(mockResponseSpec)
        whenever(mockResponseSpec.bodyToMono(Map::class.java)).thenReturn(Mono.just(mockResponse))
        whenever(mockResponseSpec.retryWhen(any())).thenReturn(mockResponseSpec)

        // When
        val result = ollamaService.extractKeywordsFromQuery(query, weatherContext, timeContext)

        // Then
        assertThat(result).isNotEmpty
        assertThat(result).contains("카페", "따뜻한", "실내", "커피", "디저트")
        assertThat(result.size).isLessThanOrEqualTo(5) // Should limit to 5 keywords
    }

    @Test
    @DisplayName("Should fallback to simple keyword extraction on error")
    fun shouldFallbackToSimpleKeywordExtractionOnError() {
        // Given
        val query = "카페에서 커피 마시고 싶어"
        
        whenever(mockWebClient.post()).thenThrow(RuntimeException("Network error"))

        // When
        val result = ollamaService.extractKeywordsFromQuery(query)

        // Then
        assertThat(result).isNotEmpty
        assertThat(result).contains("cafe", "coffee", "study", "quiet") // Should use fallback mapping
    }

    @Test
    @DisplayName("Should infer place suitability successfully")
    fun shouldInferPlaceSuitabilitySuccessfully() {
        // Given
        val placeName = "블루보틀 카페"
        val description = "조용하고 세련된 분위기의 커피전문점"
        val category = "카페"
        val openingHours = "08:00-21:00"
        
        val mockJsonResponse = """
        {
            "suitable_weather": ["clear", "rainy", "cold"],
            "suitable_time": ["morning", "afternoon", "evening"],
            "indoor": true,
            "outdoor": false
        }
        """.trim()
        val mockResponse = mapOf("response" to mockJsonResponse)
        
        whenever(mockWebClient.post()).thenReturn(mockRequestBodyUriSpec)
        whenever(mockRequestBodyUriSpec.uri(any<String>())).thenReturn(mockRequestBodyUriSpec)
        whenever(mockRequestBodyUriSpec.contentType(any())).thenReturn(mockRequestBodyUriSpec)
        whenever(mockRequestBodyUriSpec.bodyValue(any())).thenReturn(mockRequestHeadersSpec)
        whenever(mockRequestHeadersSpec.retrieve()).thenReturn(mockResponseSpec)
        whenever(mockResponseSpec.bodyToMono(Map::class.java)).thenReturn(Mono.just(mockResponse))
        whenever(mockResponseSpec.retryWhen(any())).thenReturn(mockResponseSpec)

        // When
        val result = ollamaService.inferPlaceSuitability(placeName, description, category, openingHours)

        // Then
        assertThat(result).containsKey("suitable_weather")
        assertThat(result).containsKey("suitable_time") 
        assertThat(result).containsKey("indoor")
        assertThat(result).containsKey("outdoor")
        
        val suitableWeather = result["suitable_weather"] as List<*>
        assertThat(suitableWeather).contains("clear", "rainy", "cold")
        
        assertThat(result["indoor"]).isEqualTo(true)
        assertThat(result["outdoor"]).isEqualTo(false)
    }

    @Test
    @DisplayName("Should generate fallback suitability for cafe")
    fun shouldGenerateFallbackSuitabilityForCafe() {
        // Given
        val placeName = "스타벅스"
        val description = "체인 커피전문점"
        val category = "카페"
        
        whenever(mockWebClient.post()).thenThrow(RuntimeException("Network error"))

        // When
        val result = ollamaService.inferPlaceSuitability(placeName, description, category)

        // Then
        assertThat(result).containsKey("suitable_weather")
        assertThat(result).containsKey("suitable_time")
        assertThat(result).containsKey("indoor")
        assertThat(result).containsKey("outdoor")
        
        val suitableWeather = result["suitable_weather"] as List<*>
        assertThat(suitableWeather).contains("clear", "rainy", "cold")
        
        assertThat(result["indoor"]).isEqualTo(true)
        assertThat(result["outdoor"]).isEqualTo(false)
    }

    @Test
    @DisplayName("Should generate fallback suitability for park")
    fun shouldGenerateFallbackSuitabilityForPark() {
        // Given
        val placeName = "한강공원"
        val description = "넓은 녹지공간"
        val category = "공원"
        
        whenever(mockWebClient.post()).thenThrow(RuntimeException("Network error"))

        // When
        val result = ollamaService.inferPlaceSuitability(placeName, description, category)

        // Then
        val suitableWeather = result["suitable_weather"] as List<*>
        assertThat(suitableWeather).contains("clear", "hot")
        
        assertThat(result["indoor"]).isEqualTo(false)
        assertThat(result["outdoor"]).isEqualTo(true)
    }

    @Test
    @DisplayName("Should generate MBTI descriptions successfully")
    fun shouldGenerateMbtiDescriptionsSuccessfully() {
        // Given
        val placeName = "북카페 리딩룸"
        val placeDescription = "조용하고 편안한 독서 공간"
        val category = "카페"
        
        val mockResponse = mapOf("response" to "INTJ에게 완벽한 조용하고 집중할 수 있는 공간입니다.")
        
        whenever(mockWebClient.post()).thenReturn(mockRequestBodyUriSpec)
        whenever(mockRequestBodyUriSpec.uri(any<String>())).thenReturn(mockRequestBodyUriSpec)
        whenever(mockRequestBodyUriSpec.contentType(any())).thenReturn(mockRequestBodyUriSpec)
        whenever(mockRequestBodyUriSpec.bodyValue(any())).thenReturn(mockRequestHeadersSpec)
        whenever(mockRequestHeadersSpec.retrieve()).thenReturn(mockResponseSpec)
        whenever(mockResponseSpec.bodyToMono(Map::class.java)).thenReturn(Mono.just(mockResponse))
        whenever(mockResponseSpec.retryWhen(any())).thenReturn(mockResponseSpec)

        // When
        val result = ollamaService.generateMbtiDescriptions(placeName, placeDescription, category)

        // Then
        assertThat(result).hasSize(16) // Should have descriptions for all MBTI types
        assertThat(result).containsKeys("INTJ", "INTP", "ENTJ", "ENTP", "INFJ", "INFP", "ENFJ", "ENFP",
                                        "ISTJ", "ISFJ", "ESTJ", "ESFJ", "ISTP", "ISFP", "ESTP", "ESFP")
        
        // All descriptions should be non-empty
        result.values.forEach { description ->
            assertThat(description).isNotBlank()
        }
    }

    @Test
    @DisplayName("Should generate fallback descriptions on error")
    fun shouldGenerateFallbackDescriptionsOnError() {
        // Given
        val placeName = "테스트 카페"
        val placeDescription = "테스트 설명"
        val category = "카페"
        
        whenever(mockWebClient.post()).thenThrow(RuntimeException("Network error"))

        // When
        val result = ollamaService.generateMbtiDescriptions(placeName, placeDescription, category)

        // Then
        assertThat(result).hasSize(16)
        
        // Should contain fallback descriptions
        assertThat(result["INTJ"]).contains("quiet and personal cafe")
        assertThat(result["ENTJ"]).contains("social and vibrant cafe")
    }

    @Test
    @DisplayName("Should generate consistent prompt hashes")
    fun shouldGenerateConsistentPromptHashes() {
        // Given
        val placeName = "테스트 카페"
        val placeDescription = "테스트 설명"
        val category = "카페"
        val mbti = "INTJ"

        // When
        val hash1 = ollamaService.generatePromptHash(placeName, placeDescription, category, mbti)
        val hash2 = ollamaService.generatePromptHash(placeName, placeDescription, category, mbti)

        // Then
        assertThat(hash1).isEqualTo(hash2)
        assertThat(hash1).hasSize(64) // SHA-256 hash should be 64 characters
        assertThat(hash1).matches("[a-f0-9]+") // Should be hexadecimal
    }
}