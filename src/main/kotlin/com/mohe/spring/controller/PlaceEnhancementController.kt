package com.mohe.spring.controller

import com.mohe.spring.service.PlaceDescriptionMergeService
import com.mohe.spring.service.StepByStepApiService
import com.mohe.spring.service.ApiProviderRegistry
import com.mohe.spring.dto.ApiResponse
import io.swagger.v3.oas.annotations.Operation
import io.swagger.v3.oas.annotations.Parameter
import io.swagger.v3.oas.annotations.tags.Tag
import io.swagger.v3.oas.annotations.responses.ApiResponse as SwaggerApiResponse
import org.slf4j.LoggerFactory
import org.springframework.http.ResponseEntity
import org.springframework.security.access.prepost.PreAuthorize
import org.springframework.web.bind.annotation.*
import kotlinx.coroutines.runBlocking

/**
 * 장소 정보 향상 및 API 관리를 위한 컨트롤러
 * 
 * 이 컨트롤러는 다음 기능을 제공합니다:
 * - 단계별 API 호출 및 진행 상황 추적
 * - 실시간 설명 병합 및 개선
 * - 다중 API 제공자 관리
 * - 개별 장소 정보 재처리
 */
@Tag(name = "Place Enhancement", description = "장소 정보 향상 및 API 통합 관리")
@RestController
@RequestMapping("/api/place-enhancement")
class PlaceEnhancementController(
    private val descriptionMergeService: PlaceDescriptionMergeService,
    private val stepByStepApiService: StepByStepApiService,
    private val apiProviderRegistry: ApiProviderRegistry
) {

    private val logger = LoggerFactory.getLogger(PlaceEnhancementController::class.java)

    @Operation(
        summary = "단계별 장소 정보 수집",
        description = "개별 장소에 대해 Naver → Google → 병합 순서로 단계별 API 호출을 실행합니다."
    )
    @SwaggerApiResponse(responseCode = "200", description = "처리 진행 상황 반환")
    @PostMapping("/process-step-by-step")
    fun processPlaceStepByStep(
        @Parameter(description = "장소명", required = true)
        @RequestParam placeName: String,
        @Parameter(description = "주소 (선택사항)")
        @RequestParam(required = false) address: String?,
        @Parameter(description = "위도 (선택사항)")
        @RequestParam(required = false) latitude: Double?,
        @Parameter(description = "경도 (선택사항)")
        @RequestParam(required = false) longitude: Double?
    ): ResponseEntity<ApiResponse<StepByStepApiService.PlaceProcessingProgress>> {
        
        return try {
            logger.info("Starting step-by-step processing for place: $placeName")
            
            val progress = runBlocking {
                stepByStepApiService.processPlaceStepByStep(placeName, address, latitude, longitude)
            }
            
            logger.info("Step-by-step processing completed for $placeName with status: ${progress.overallStatus}")
            
            ResponseEntity.ok(ApiResponse.success(progress))
            
        } catch (e: Exception) {
            logger.error("Error processing place $placeName: ${e.message}", e)
            ResponseEntity.badRequest().body(
                ApiResponse.error("PROCESSING_ERROR", "장소 처리 중 오류가 발생했습니다: ${e.message}")
            )
        }
    }

    @Operation(
        summary = "실패한 단계 재처리",
        description = "이전 처리에서 실패한 단계들만 선별하여 재시도합니다."
    )
    @SwaggerApiResponse(responseCode = "200", description = "재처리 결과")
    @PostMapping("/retry-failed-steps")
    fun retryFailedSteps(
        @Parameter(description = "재처리할 진행 상황 객체", required = true)
        @RequestBody progress: StepByStepApiService.PlaceProcessingProgress
    ): ResponseEntity<ApiResponse<StepByStepApiService.PlaceProcessingProgress>> {
        
        return try {
            logger.info("Retrying failed steps for place: ${progress.placeName}")
            
            val updatedProgress = runBlocking {
                stepByStepApiService.retryFailedSteps(progress)
            }
            
            logger.info("Retry completed for ${progress.placeName}, new status: ${updatedProgress.overallStatus}")
            
            ResponseEntity.ok(ApiResponse.success(updatedProgress))
            
        } catch (e: Exception) {
            logger.error("Error retrying steps for ${progress.placeName}: ${e.message}", e)
            ResponseEntity.badRequest().body(
                ApiResponse.error("RETRY_ERROR", "재처리 중 오류가 발생했습니다: ${e.message}")
            )
        }
    }

    @Operation(
        summary = "설명 병합 미리보기",
        description = "실제 데이터베이스 저장 없이 설명 병합 결과를 미리 확인합니다."
    )
    @SwaggerApiResponse(responseCode = "200", description = "병합된 설명 정보")
    @PostMapping("/preview-description")
    fun previewMergedDescription(
        @Parameter(description = "네이버 장소 데이터", required = true)
        @RequestBody naverPlaceData: Map<String, Any>,
        @Parameter(description = "구글 장소 데이터 (선택사항)")
        @RequestBody(required = false) googlePlaceData: Map<String, Any>?
    ): ResponseEntity<ApiResponse<PlaceDescriptionMergeService.MergedDescription>> {
        
        return try {
            // Map 데이터를 실제 DTO로 변환하는 로직 필요
            // 여기서는 간단한 Mock 객체 생성
            val naverPlace = createMockNaverPlace(naverPlaceData)
            val googlePlace = googlePlaceData?.let { createMockGooglePlace(it) }
            
            val mergedDescription = descriptionMergeService.mergeDescriptions(naverPlace, googlePlace)
            
            logger.debug("Generated preview description with style: ${mergedDescription.style}")
            
            ResponseEntity.ok(ApiResponse.success(mergedDescription))
            
        } catch (e: Exception) {
            logger.error("Error previewing description merge: ${e.message}", e)
            ResponseEntity.badRequest().body(
                ApiResponse.error("PREVIEW_ERROR", "설명 병합 미리보기 중 오류가 발생했습니다: ${e.message}")
            )
        }
    }

    @Operation(
        summary = "등록된 API 제공자 목록 조회",
        description = "현재 시스템에 등록된 모든 API 제공자와 처리 전략을 조회합니다."
    )
    @SwaggerApiResponse(responseCode = "200", description = "등록된 제공자 및 전략 목록")
    @GetMapping("/providers")
    fun getAvailableProviders(): ResponseEntity<ApiResponse<Map<String, Set<String>>>> {
        
        return try {
            val result = mapOf(
                "providers" to apiProviderRegistry.getAvailableProviders(),
                "strategies" to apiProviderRegistry.getAvailableStrategies()
            )
            
            ResponseEntity.ok(ApiResponse.success(result))
            
        } catch (e: Exception) {
            logger.error("Error getting providers: ${e.message}", e)
            ResponseEntity.badRequest().body(
                ApiResponse.error("PROVIDER_ERROR", "제공자 목록 조회 중 오류가 발생했습니다: ${e.message}")
            )
        }
    }

    @Operation(
        summary = "다중 제공자 데이터 수집",
        description = "여러 API 제공자로부터 동시에 장소 정보를 수집합니다."
    )
    @SwaggerApiResponse(responseCode = "200", description = "다중 제공자 수집 결과")
    @PreAuthorize("hasRole('ADMIN')")
    @PostMapping("/fetch-from-all-providers")
    fun fetchFromAllProviders(
        @Parameter(description = "장소명", required = true)
        @RequestParam placeName: String,
        @Parameter(description = "주소 (선택사항)")
        @RequestParam(required = false) address: String?,
        @Parameter(description = "사용할 제공자 필터 (콤마 구분)")
        @RequestParam(required = false) providerFilter: String?
    ): ResponseEntity<ApiResponse<Map<String, Any>>> {
        
        return try {
            logger.info("Fetching data from all providers for place: $placeName")
            
            val filterSet = providerFilter?.split(",")?.map { it.trim() }?.toSet()
            
            val results = runBlocking {
                apiProviderRegistry.fetchFromAllProviders(placeName, address, filterSet)
            }
            
            val response = mapOf(
                "placeName" to placeName,
                "results" to results,
                "totalProviders" to results.size,
                "successfulProviders" to results.count { it.value.isSuccess() }
            )
            
            ResponseEntity.ok(ApiResponse.success(response))
            
        } catch (e: Exception) {
            logger.error("Error fetching from all providers for $placeName: ${e.message}", e)
            ResponseEntity.badRequest().body(
                ApiResponse.error("MULTI_FETCH_ERROR", "다중 제공자 데이터 수집 중 오류가 발생했습니다: ${e.message}")
            )
        }
    }

    // Helper methods for mock object creation
    private fun createMockNaverPlace(data: Map<String, Any>): Any {
        // 실제 구현에서는 Jackson ObjectMapper를 사용하여 변환
        return data // Temporary implementation
    }

    private fun createMockGooglePlace(data: Map<String, Any>): Any {
        // 실제 구현에서는 Jackson ObjectMapper를 사용하여 변환
        return data // Temporary implementation
    }
}