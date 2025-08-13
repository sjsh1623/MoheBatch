package com.mohe.spring.controller

import com.mohe.spring.dto.ApiResponse
import com.mohe.spring.service.EnhancedRecommendationService
import com.mohe.spring.service.EnhancedRecommendationsResponse
import com.mohe.spring.security.UserPrincipal
import com.mohe.spring.repository.UserRepository
import io.swagger.v3.oas.annotations.Operation
import io.swagger.v3.oas.annotations.Parameter
import io.swagger.v3.oas.annotations.responses.ApiResponse as SwaggerApiResponse
import io.swagger.v3.oas.annotations.responses.ApiResponses
import io.swagger.v3.oas.annotations.tags.Tag
import org.slf4j.LoggerFactory
import org.springframework.http.ResponseEntity
import org.springframework.security.core.annotation.AuthenticationPrincipal
import org.springframework.web.bind.annotation.*

/**
 * Enhanced recommendations controller using MBTI-weighted similarity calculations
 */
@RestController
@RequestMapping("/api/recommendations")
@Tag(name = "Enhanced Recommendations", description = "MBTI-weighted similarity-based place recommendations")
class RecommendationController(
    private val enhancedRecommendationService: EnhancedRecommendationService,
    private val userRepository: UserRepository
) {

    private val logger = LoggerFactory.getLogger(RecommendationController::class.java)

    @Operation(
        summary = "Get enhanced recommendations",
        description = "Get personalized place recommendations using MBTI-weighted similarity calculations based on user's bookmark history"
    )
    @ApiResponses(
        value = [
            SwaggerApiResponse(responseCode = "200", description = "Enhanced recommendations retrieved successfully"),
            SwaggerApiResponse(responseCode = "400", description = "Invalid parameters"),
            SwaggerApiResponse(responseCode = "401", description = "User not authenticated"),
            SwaggerApiResponse(responseCode = "500", description = "Failed to generate recommendations")
        ]
    )
    @GetMapping("/enhanced")
    fun getEnhancedRecommendations(
        @AuthenticationPrincipal userPrincipal: UserPrincipal,
        @Parameter(description = "Maximum number of recommendations to return (default: 15, max: 50)")
        @RequestParam(defaultValue = "15") limit: Int,
        @Parameter(description = "Whether to exclude already bookmarked places (default: true)")
        @RequestParam(defaultValue = "true") excludeBookmarked: Boolean
    ): ResponseEntity<ApiResponse<EnhancedRecommendationsResponse>> {
        return try {
            if (limit < 1 || limit > 50) {
                return ResponseEntity.badRequest().body(
                    ApiResponse.error("VALIDATION_ERROR", "Limit must be between 1 and 50")
                )
            }

            val user = userRepository.findById(userPrincipal.id)
                .orElseThrow { RuntimeException("사용자를 찾을 수 없습니다") }

            logger.info("Generating enhanced recommendations for user ${user.id} with limit $limit")

            val recommendations = enhancedRecommendationService.getEnhancedRecommendations(
                user = user,
                limit = limit,
                excludeBookmarked = excludeBookmarked
            )

            ResponseEntity.ok(ApiResponse.success(recommendations))

        } catch (ex: Exception) {
            logger.error("Failed to get enhanced recommendations for user ${userPrincipal.id}", ex)
            ResponseEntity.status(500).body(
                ApiResponse.error("INTERNAL_SERVER_ERROR", "Failed to generate recommendations: ${ex.message}")
            )
        }
    }

    @Operation(
        summary = "Get MBTI-specific recommendations",
        description = "Get recommendations specifically tailored to a particular MBTI type (useful for exploring different personality preferences)"
    )
    @ApiResponses(
        value = [
            SwaggerApiResponse(responseCode = "200", description = "MBTI-specific recommendations retrieved successfully"),
            SwaggerApiResponse(responseCode = "400", description = "Invalid MBTI type or parameters"),
            SwaggerApiResponse(responseCode = "401", description = "User not authenticated"),
            SwaggerApiResponse(responseCode = "500", description = "Failed to generate recommendations")
        ]
    )
    @GetMapping("/mbti/{mbtiType}")
    fun getMbtiSpecificRecommendations(
        @AuthenticationPrincipal userPrincipal: UserPrincipal,
        @Parameter(description = "MBTI type (e.g., INTJ, ENFP)", required = true)
        @PathVariable mbtiType: String,
        @Parameter(description = "Maximum number of recommendations to return (default: 15, max: 50)")
        @RequestParam(defaultValue = "15") limit: Int
    ): ResponseEntity<ApiResponse<EnhancedRecommendationsResponse>> {
        return try {
            if (!isValidMbtiType(mbtiType)) {
                return ResponseEntity.badRequest().body(
                    ApiResponse.error("VALIDATION_ERROR", "Invalid MBTI type: $mbtiType")
                )
            }

            if (limit < 1 || limit > 50) {
                return ResponseEntity.badRequest().body(
                    ApiResponse.error("VALIDATION_ERROR", "Limit must be between 1 and 50")
                )
            }

            val user = userRepository.findById(userPrincipal.id)
                .orElseThrow { RuntimeException("사용자를 찾을 수 없습니다") }

            // Create a temporary user object with the specified MBTI type
            val mbtiUser = user.copy(mbti = mbtiType.uppercase())

            logger.info("Generating MBTI-specific recommendations for user ${user.id} with MBTI $mbtiType")

            val recommendations = enhancedRecommendationService.getEnhancedRecommendations(
                user = mbtiUser,
                limit = limit,
                excludeBookmarked = true
            )

            ResponseEntity.ok(ApiResponse.success(recommendations))

        } catch (ex: Exception) {
            logger.error("Failed to get MBTI-specific recommendations for user ${userPrincipal.id} with MBTI $mbtiType", ex)
            ResponseEntity.status(500).body(
                ApiResponse.error("INTERNAL_SERVER_ERROR", "Failed to generate MBTI-specific recommendations: ${ex.message}")
            )
        }
    }

    @Operation(
        summary = "Get recommendation explanation",
        description = "Get detailed explanation of why specific places were recommended to the user"
    )
    @ApiResponses(
        value = [
            SwaggerApiResponse(responseCode = "200", description = "Recommendation explanation retrieved successfully"),
            SwaggerApiResponse(responseCode = "401", description = "User not authenticated"),
            SwaggerApiResponse(responseCode = "500", description = "Failed to generate explanation")
        ]
    )
    @GetMapping("/explanation")
    fun getRecommendationExplanation(
        @AuthenticationPrincipal userPrincipal: UserPrincipal
    ): ResponseEntity<ApiResponse<Map<String, Any>>> {
        return try {
            val user = userRepository.findById(userPrincipal.id)
                .orElseThrow { RuntimeException("사용자를 찾을 수 없습니다") }

            val explanation = mapOf(
                "userId" to user.id,
                "userMbti" to (user.mbti ?: "unknown"),
                "algorithm" to "mbti_similarity_based",
                "explanation" to mapOf(
                    "step1" to "사용자의 북마크된 장소들을 분석합니다",
                    "step2" to "북마크된 장소와 유사한 다른 장소들을 찾습니다",
                    "step3" to "MBTI 성향에 따라 가중치를 적용합니다",
                    "step4" to "다양성과 인기도 균형을 맞춰 최종 추천 목록을 생성합니다"
                ),
                "factors" to mapOf(
                    "similarity" to "북마크 기반 유사도 (자카드, 코사인 유사도)",
                    "mbti" to "MBTI 성향별 장소 선호도 가중치",
                    "diversity" to "카테고리와 지역 다양성 보장",
                    "freshness" to "최근 데이터에 더 높은 가중치 부여",
                    "popularity" to "인기 편향 완화를 위한 패널티 적용"
                )
            )

            ResponseEntity.ok(ApiResponse.success(explanation))

        } catch (ex: Exception) {
            logger.error("Failed to get recommendation explanation for user ${userPrincipal.id}", ex)
            ResponseEntity.status(500).body(
                ApiResponse.error("INTERNAL_SERVER_ERROR", "Failed to generate explanation: ${ex.message}")
            )
        }
    }

    /**
     * Validate MBTI type format
     */
    private fun isValidMbtiType(mbti: String): Boolean {
        val validMbtiTypes = setOf(
            "INTJ", "INTP", "ENTJ", "ENTP",
            "INFJ", "INFP", "ENFJ", "ENFP",
            "ISTJ", "ISFJ", "ESTJ", "ESFJ",
            "ISTP", "ISFP", "ESTP", "ESFP"
        )
        return mbti.uppercase() in validMbtiTypes
    }
}