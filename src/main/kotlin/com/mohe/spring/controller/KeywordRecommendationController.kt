package com.mohe.spring.controller

import com.mohe.spring.dto.ApiResponse
import com.mohe.spring.dto.ErrorCode
import com.mohe.spring.service.KeywordExtractionService
import com.mohe.spring.service.PlaceService
import com.mohe.spring.repository.PlaceKeywordExtractionRepository
import io.swagger.v3.oas.annotations.Operation
import io.swagger.v3.oas.annotations.Parameter
import io.swagger.v3.oas.annotations.media.Content
import io.swagger.v3.oas.annotations.media.Schema
import io.swagger.v3.oas.annotations.responses.ApiResponse as SwaggerApiResponse
import io.swagger.v3.oas.annotations.responses.ApiResponses
import io.swagger.v3.oas.annotations.tags.Tag
import jakarta.servlet.http.HttpServletRequest
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.*
import java.math.BigDecimal

@RestController
@RequestMapping("/api/keyword")
@Tag(name = "키워드 기반 추천", description = "키워드 벡터 유사도를 이용한 장소 추천 API")
class KeywordRecommendationController(
    private val keywordExtractionService: KeywordExtractionService,
    private val placeService: PlaceService,
    private val placeKeywordExtractionRepository: PlaceKeywordExtractionRepository
) {

    @GetMapping("/places/{placeId}/similar")
    @Operation(
        summary = "키워드 유사도 기반 장소 추천",
        description = "특정 장소와 키워드 벡터 유사도가 높은 다른 장소들을 추천합니다."
    )
    @ApiResponses(
        value = [
            SwaggerApiResponse(
                responseCode = "200",
                description = "유사 장소 추천 성공",
                content = [Content(
                    mediaType = "application/json",
                    schema = Schema(implementation = KeywordSimilarPlacesResponse::class)
                )]
            ),
            SwaggerApiResponse(
                responseCode = "404",
                description = "장소를 찾을 수 없음"
            ),
            SwaggerApiResponse(
                responseCode = "400",
                description = "키워드 추출 데이터가 없음"
            )
        ]
    )
    fun getSimilarPlacesByKeywords(
        @Parameter(description = "기준 장소 ID", required = true)
        @PathVariable placeId: Long,
        @Parameter(description = "추천할 장소 수", required = false)
        @RequestParam(defaultValue = "10") limit: Int,
        @Parameter(description = "최소 유사도 점수", required = false)
        @RequestParam(defaultValue = "0.3") minSimilarity: Double,
        httpRequest: HttpServletRequest
    ): ResponseEntity<ApiResponse<KeywordSimilarPlacesResponse>> {
        return try {
            // Check if place exists
            val place = placeService.getPlaceById(placeId)
                ?: return ResponseEntity.badRequest().body(
                    ApiResponse.error(
                        code = ErrorCode.NOT_FOUND,
                        message = "장소를 찾을 수 없습니다",
                        path = httpRequest.requestURI
                    )
                )

            // Check if keyword extraction exists for this place
            val keywordExtraction = placeKeywordExtractionRepository.findByPlaceId(placeId)
                ?: return ResponseEntity.badRequest().body(
                    ApiResponse.error(
                        code = ErrorCode.INVALID_REQUEST,
                        message = "해당 장소의 키워드 추출 데이터가 없습니다",
                        path = httpRequest.requestURI
                    )
                )

            // Find similar places using keyword vector similarity
            val similarPlaceIds = keywordExtractionService.findSimilarPlacesByKeywords(placeId, limit * 2) // Get more to filter
            
            // Get place details and calculate similarity scores
            val similarPlacesWithScores = placeKeywordExtractionRepository.findSimilarPlacesWithScores(placeId, limit * 2)
                .filter { result -> 
                    val similarityScore = (result[1] as? Number)?.toDouble() ?: 0.0
                    similarityScore >= minSimilarity
                }
                .take(limit)
                .map { result ->
                    val similarPlaceId = (result[0] as? Number)?.toLong() ?: 0L
                    val similarityScore = (result[1] as? Number)?.toDouble() ?: 0.0
                    val similarPlace = placeService.getPlaceById(similarPlaceId)
                    
                    if (similarPlace != null) {
                        KeywordSimilarPlace(
                            placeId = similarPlace.id!!,
                            name = similarPlace.name,
                            address = similarPlace.address ?: "",
                            category = similarPlace.category ?: "",
                            rating = similarPlace.rating ?: BigDecimal.ZERO,
                            imageUrl = similarPlace.imageUrl,
                            similarityScore = BigDecimal(similarityScore.toString()),
                            sharedKeywords = getSharedKeywords(placeId, similarPlaceId)
                        )
                    } else null
                }
                .filterNotNull()

            val response = KeywordSimilarPlacesResponse(
                basePlaceId = placeId,
                basePlaceName = place.name,
                totalSimilarPlaces = similarPlacesWithScores.size,
                minSimilarityScore = BigDecimal(minSimilarity.toString()),
                similarPlaces = similarPlacesWithScores
            )

            ResponseEntity.ok(ApiResponse.success(response))
            
        } catch (e: Exception) {
            ResponseEntity.badRequest().body(
                ApiResponse.error(
                    code = ErrorCode.INTERNAL_SERVER_ERROR,
                    message = "유사 장소 추천 중 오류가 발생했습니다: ${e.message}",
                    path = httpRequest.requestURI
                )
            )
        }
    }

    @PostMapping("/places/{placeId}/extract")
    @Operation(
        summary = "장소 키워드 추출",
        description = "특정 장소에 대해 키워드 추출을 수행합니다."
    )
    fun extractKeywordsForPlace(
        @Parameter(description = "장소 ID", required = true)
        @PathVariable placeId: Long,
        httpRequest: HttpServletRequest
    ): ResponseEntity<ApiResponse<KeywordExtractionResponse>> {
        return try {
            val place = placeService.getPlaceById(placeId)
                ?: return ResponseEntity.badRequest().body(
                    ApiResponse.error(
                        code = ErrorCode.NOT_FOUND,
                        message = "장소를 찾을 수 없습니다",
                        path = httpRequest.requestURI
                    )
                )

            // Build description for keyword extraction
            val description = listOfNotNull(
                place.description,
                place.category?.let { "카테고리: $it" },
                place.address?.let { "주소: $it" }
            ).joinToString(". ")

            val result = keywordExtractionService.extractKeywords(
                placeId = placeId,
                placeName = place.name,
                placeDescription = description,
                category = place.category ?: ""
            )

            val response = KeywordExtractionResponse(
                placeId = placeId,
                placeName = place.name,
                extractedKeywords = result.selectedKeywords.map { 
                    ExtractedKeyword(
                        keyword = it.keyword,
                        confidence = BigDecimal(it.confidence.toString()),
                        reasoning = it.reasoning
                    )
                },
                processingTimeMs = result.processingTimeMs,
                vectorDimensions = 100,
                totalKeywordsSelected = result.selectedKeywords.size
            )

            ResponseEntity.ok(ApiResponse.success(response))
            
        } catch (e: Exception) {
            ResponseEntity.badRequest().body(
                ApiResponse.error(
                    code = ErrorCode.INTERNAL_SERVER_ERROR,
                    message = "키워드 추출 중 오류가 발생했습니다: ${e.message}",
                    path = httpRequest.requestURI
                )
            )
        }
    }

    private fun getSharedKeywords(placeId1: Long, placeId2: Long): List<String> {
        return try {
            val extraction1 = placeKeywordExtractionRepository.findByPlaceId(placeId1)
            val extraction2 = placeKeywordExtractionRepository.findByPlaceId(placeId2)
            
            if (extraction1 != null && extraction2 != null) {
                // Parse selected keywords JSON and find common ones
                val keywords1 = parseSelectedKeywords(extraction1.selectedKeywords)
                val keywords2 = parseSelectedKeywords(extraction2.selectedKeywords)
                
                keywords1.filter { keyword1 ->
                    keywords2.any { keyword2 -> keyword1.lowercase() == keyword2.lowercase() }
                }.take(5) // Limit to top 5 shared keywords
            } else {
                emptyList()
            }
        } catch (e: Exception) {
            emptyList()
        }
    }
    
    private fun parseSelectedKeywords(jsonString: String): List<String> {
        return try {
            // Simple JSON parsing to extract keyword names
            // In production, use proper JSON parsing
            val keywords = mutableListOf<String>()
            val keywordPattern = """"keyword":\s*"([^"]+)"""".toRegex()
            keywordPattern.findAll(jsonString).forEach { match ->
                keywords.add(match.groupValues[1])
            }
            keywords
        } catch (e: Exception) {
            emptyList()
        }
    }
}

// Response DTOs
data class KeywordSimilarPlacesResponse(
    val basePlaceId: Long,
    val basePlaceName: String,
    val totalSimilarPlaces: Int,
    val minSimilarityScore: BigDecimal,
    val similarPlaces: List<KeywordSimilarPlace>
)

data class KeywordSimilarPlace(
    val placeId: Long,
    val name: String,
    val address: String,
    val category: String,
    val rating: BigDecimal,
    val imageUrl: String?,
    val similarityScore: BigDecimal,
    val sharedKeywords: List<String>
)

data class KeywordExtractionResponse(
    val placeId: Long,
    val placeName: String,
    val extractedKeywords: List<ExtractedKeyword>,
    val processingTimeMs: Long,
    val vectorDimensions: Int,
    val totalKeywordsSelected: Int
)

data class ExtractedKeyword(
    val keyword: String,
    val confidence: BigDecimal,
    val reasoning: String?
)