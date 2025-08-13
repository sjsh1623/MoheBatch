package com.mohe.spring.controller

import com.mohe.spring.dto.ApiResponse
import com.mohe.spring.service.VectorSimilarityService
import com.mohe.spring.service.KeywordExtractionService
import com.mohe.spring.security.UserPrincipal
import io.swagger.v3.oas.annotations.Operation
import io.swagger.v3.oas.annotations.Parameter
import io.swagger.v3.oas.annotations.tags.Tag
import org.springframework.http.ResponseEntity
import org.springframework.security.access.prepost.PreAuthorize
import org.springframework.security.core.context.SecurityContextHolder
import org.springframework.web.bind.annotation.*

@RestController
@RequestMapping("/api/vector")
@Tag(name = "벡터 관리", description = "사용자 선호도 벡터 및 장소 벡터 관리")
class VectorController(
    private val vectorSimilarityService: VectorSimilarityService,
    private val keywordExtractionService: KeywordExtractionService
) {
    
    @PostMapping("/user/regenerate")
    @PreAuthorize("hasRole('USER')")
    @Operation(
        summary = "사용자 선호도 벡터 재생성",
        description = "현재 사용자의 프로필과 선호도를 기반으로 벡터를 재생성합니다."
    )
    fun regenerateUserVector(): ResponseEntity<ApiResponse<UserVectorResponse>> {
        val currentUser = getCurrentUser()
        
        val vector = vectorSimilarityService.generateUserPreferenceVector(
            userId = currentUser.id,
            forceRegeneration = true
        )
        
        return ResponseEntity.ok(
            ApiResponse.success(
                UserVectorResponse(
                    vectorId = vector.id,
                    keywordCount = vector.getSelectedKeywordsList().size,
                    confidence = vector.getSelectedKeywordsList().map { it.confidence }.average(),
                    keywords = vector.getSelectedKeywordsList().map { "${it.keyword}(${String.format("%.2f", it.confidence)})" },
                    extractionSource = vector.extractionSource,
                    modelName = vector.modelName,
                    createdAt = vector.createdAt.toString()
                )
            )
        )
    }
    
    @PostMapping("/place/{placeId}/regenerate")
    @PreAuthorize("hasRole('ADMIN')")
    @Operation(
        summary = "장소 벡터 재생성", 
        description = "특정 장소의 설명을 기반으로 벡터를 재생성합니다."
    )
    fun regeneratePlaceVector(
        @Parameter(description = "장소 ID")
        @PathVariable placeId: Long
    ): ResponseEntity<ApiResponse<PlaceVectorResponse>> {
        
        val vector = vectorSimilarityService.generatePlaceDescriptionVector(
            placeId = placeId,
            forceRegeneration = true
        )
        
        return ResponseEntity.ok(
            ApiResponse.success(
                PlaceVectorResponse(
                    vectorId = vector.id,
                    placeId = placeId,
                    placeName = vector.place.name,
                    keywordCount = vector.getSelectedKeywordsList().size,
                    confidence = vector.getSelectedKeywordsList().map { it.confidence }.average(),
                    keywords = vector.getSelectedKeywordsList().map { "${it.keyword}(${String.format("%.2f", it.confidence)})" },
                    extractionSource = vector.extractionSource,
                    modelName = vector.modelName,
                    createdAt = vector.createdAt.toString()
                )
            )
        )
    }
    
    @GetMapping("/similarity/places")
    @PreAuthorize("hasRole('USER')")
    @Operation(
        summary = "벡터 기반 장소 추천",
        description = "현재 사용자의 벡터와 유사한 장소들을 추천합니다."
    )
    fun getVectorBasedRecommendations(
        @Parameter(description = "추천 개수")
        @RequestParam(defaultValue = "20") limit: Int,
        @Parameter(description = "제외할 장소 ID 목록")
        @RequestParam(required = false) excludeIds: List<Long>?
    ): ResponseEntity<ApiResponse<VectorRecommendationsResponse>> {
        
        val currentUser = getCurrentUser()
        val excludePlaceIds = excludeIds ?: emptyList()
        
        val matches = vectorSimilarityService.getTopSimilarPlacesForUser(
            userId = currentUser.id,
            limit = limit,
            excludePlaceIds = excludePlaceIds,
            minSimilarityThreshold = 0.1
        )
        
        val recommendations = matches.map { match ->
            VectorRecommendation(
                placeId = match.place.id.toString(),
                placeName = match.place.name,
                category = match.place.category,
                location = match.place.location,
                image = match.place.imageUrl,
                similarityScore = String.format("%.3f", match.getSimilarityScore()),
                cosineSimilarity = String.format("%.3f", match.similarity.getCosineSimilarityAsDouble()),
                jaccardSimilarity = String.format("%.3f", match.similarity.getJaccardSimilarityAsDouble()),
                mbtiBoost = String.format("%.2f", match.similarity.getMbtiBoostFactorAsDouble()),
                matchingKeywords = match.matchingKeywords,
                recommendationReason = match.recommendationReason
            )
        }
        
        return ResponseEntity.ok(
            ApiResponse.success(
                VectorRecommendationsResponse(
                    recommendations = recommendations,
                    totalCount = recommendations.size,
                    userVectorExists = true,
                    averageSimilarity = if (recommendations.isNotEmpty()) 
                        recommendations.map { it.similarityScore.toDouble() }.average() else 0.0
                )
            )
        )
    }
    
    @PostMapping("/similarity/calculate")
    @PreAuthorize("hasRole('USER')")
    @Operation(
        summary = "사용자-장소 유사도 계산",
        description = "현재 사용자와 특정 장소 간의 벡터 유사도를 계산합니다."
    )
    fun calculateSimilarity(
        @Parameter(description = "장소 ID")
        @RequestParam placeId: Long,
        @Parameter(description = "캐시 사용 여부")
        @RequestParam(defaultValue = "true") useCache: Boolean
    ): ResponseEntity<ApiResponse<SimilarityResult>> {
        
        val currentUser = getCurrentUser()
        
        val similarity = vectorSimilarityService.calculateUserPlaceSimilarity(
            userId = currentUser.id,
            placeId = placeId,
            useCache = useCache
        )
        
        return ResponseEntity.ok(
            ApiResponse.success(
                SimilarityResult(
                    userId = currentUser.id,
                    placeId = placeId,
                    cosineSimilarity = String.format("%.4f", similarity.getCosineSimilarityAsDouble()),
                    jaccardSimilarity = String.format("%.4f", similarity.getJaccardSimilarityAsDouble()),
                    euclideanDistance = String.format("%.4f", similarity.euclideanDistance.toDouble()),
                    mbtiBoostFactor = String.format("%.2f", similarity.getMbtiBoostFactorAsDouble()),
                    weightedSimilarity = String.format("%.4f", similarity.getWeightedSimilarityAsDouble()),
                    commonKeywords = similarity.commonKeywords,
                    keywordOverlapRatio = String.format("%.2f", similarity.keywordOverlapRatio?.toDouble() ?: 0.0),
                    calculatedAt = similarity.calculatedAt.toString()
                )
            )
        )
    }
    
    @PostMapping("/test/extract")
    @PreAuthorize("hasRole('ADMIN')")
    @Operation(
        summary = "키워드 추출 테스트",
        description = "텍스트에서 키워드를 추출하는 기능을 테스트합니다."
    )
    fun testKeywordExtraction(
        @RequestBody request: KeywordExtractionTestRequest
    ): ResponseEntity<ApiResponse<KeywordExtractionTestResponse>> {
        
        val result = keywordExtractionService.extractKeywords(
            placeId = 0L, // Test mode
            placeName = "테스트 장소",
            placeDescription = request.text,
            category = request.contextType ?: "테스트"
        )
        
        return ResponseEntity.ok(
            ApiResponse.success(
                KeywordExtractionTestResponse(
                    originalText = request.text,
                    normalizedText = request.text, // Use input text as normalized text
                    selectedKeywords = result.selectedKeywords.map { 
                        "${it.keyword}(${String.format("%.3f", it.confidence)})" 
                    },
                    vectorDimension = result.vectorArray.size,
                    nonZeroValues = result.vectorArray.count { it != 0.0f },
                    extractionSource = "ollama-api",
                    modelName = "keyword-extraction-model",
                    overallConfidence = String.format("%.3f", result.selectedKeywords.map { it.confidence }.average())
                )
            )
        )
    }
    
    private fun getCurrentUser(): UserPrincipal {
        return SecurityContextHolder.getContext().authentication.principal as UserPrincipal
    }
}

// Response DTOs
data class UserVectorResponse(
    val vectorId: Long,
    val keywordCount: Int,
    val confidence: Double,
    val keywords: List<String>,
    val extractionSource: String,
    val modelName: String,
    val createdAt: String
)

data class PlaceVectorResponse(
    val vectorId: Long,
    val placeId: Long,
    val placeName: String,
    val keywordCount: Int,
    val confidence: Double,
    val keywords: List<String>,
    val extractionSource: String,
    val modelName: String,
    val createdAt: String
)

data class VectorRecommendationsResponse(
    val recommendations: List<VectorRecommendation>,
    val totalCount: Int,
    val userVectorExists: Boolean,
    val averageSimilarity: Double
)

data class VectorRecommendation(
    val placeId: String,
    val placeName: String,
    val category: String?,
    val location: String?,
    val image: String?,
    val similarityScore: String,
    val cosineSimilarity: String,
    val jaccardSimilarity: String,
    val mbtiBoost: String,
    val matchingKeywords: List<String>,
    val recommendationReason: String
)

data class SimilarityResult(
    val userId: Long,
    val placeId: Long,
    val cosineSimilarity: String,
    val jaccardSimilarity: String,
    val euclideanDistance: String,
    val mbtiBoostFactor: String,
    val weightedSimilarity: String,
    val commonKeywords: Int,
    val keywordOverlapRatio: String,
    val calculatedAt: String
)

data class KeywordExtractionTestRequest(
    val text: String,
    val contextType: String = "place", // "place" or "user"
    val mbti: String? = null
)

data class KeywordExtractionTestResponse(
    val originalText: String,
    val normalizedText: String,
    val selectedKeywords: List<String>,
    val vectorDimension: Int,
    val nonZeroValues: Int,
    val extractionSource: String,
    val modelName: String,
    val overallConfidence: String
)