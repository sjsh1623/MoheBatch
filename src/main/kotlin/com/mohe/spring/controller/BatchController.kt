package com.mohe.spring.controller

import com.mohe.spring.dto.ApiResponse
import com.mohe.spring.dto.ErrorCode
import com.mohe.spring.service.BatchService
import io.swagger.v3.oas.annotations.Operation
import io.swagger.v3.oas.annotations.Parameter
import io.swagger.v3.oas.annotations.media.Content
import io.swagger.v3.oas.annotations.media.Schema
import io.swagger.v3.oas.annotations.responses.ApiResponse as SwaggerApiResponse
import io.swagger.v3.oas.annotations.responses.ApiResponses
import io.swagger.v3.oas.annotations.tags.Tag
import jakarta.servlet.http.HttpServletRequest
import jakarta.validation.Valid
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.*
import java.time.LocalDateTime

@RestController
@RequestMapping("/api/batch")
@Tag(name = "배치 데이터 수집", description = "배치 프로세스를 위한 데이터 수집 API")
class BatchController(
    private val batchService: BatchService
) {

    @PostMapping("/places")
    @Operation(
        summary = "배치 장소 데이터 수집",
        description = "외부 API에서 수집한 장소 데이터를 처리하여 저장합니다."
    )
    @ApiResponses(
        value = [
            SwaggerApiResponse(
                responseCode = "200",
                description = "배치 데이터 수집 성공",
                content = [Content(
                    mediaType = "application/json",
                    schema = Schema(implementation = BatchPlaceResponse::class)
                )]
            ),
            SwaggerApiResponse(
                responseCode = "400",
                description = "잘못된 요청 데이터"
            )
        ]
    )
    fun ingestPlaceData(
        @Parameter(description = "장소 데이터 배열", required = true)
        @Valid @RequestBody placeDataList: List<BatchPlaceRequest>,
        httpRequest: HttpServletRequest
    ): ResponseEntity<ApiResponse<BatchPlaceResponse>> {
        return try {
            val response = batchService.ingestPlaceData(placeDataList)
            ResponseEntity.ok(ApiResponse.success(response))
        } catch (e: IllegalArgumentException) {
            ResponseEntity.badRequest().body(
                ApiResponse.error(
                    code = ErrorCode.INVALID_REQUEST,
                    message = e.message ?: "잘못된 장소 데이터입니다",
                    path = httpRequest.requestURI
                )
            )
        } catch (e: Exception) {
            ResponseEntity.badRequest().body(
                ApiResponse.error(
                    code = ErrorCode.INTERNAL_SERVER_ERROR,
                    message = "장소 데이터 수집 중 오류가 발생했습니다",
                    path = httpRequest.requestURI
                )
            )
        }
    }

    @PostMapping("/users")
    @Operation(
        summary = "배치 사용자 데이터 수집",
        description = "외부 API에서 수집한 사용자 데이터를 처리하여 저장합니다."
    )
    fun ingestUserData(
        @Parameter(description = "사용자 데이터 배열", required = true)
        @Valid @RequestBody userDataList: List<BatchUserRequest>,
        httpRequest: HttpServletRequest
    ): ResponseEntity<ApiResponse<BatchUserResponse>> {
        return try {
            val response = batchService.ingestUserData(userDataList)
            ResponseEntity.ok(ApiResponse.success(response))
        } catch (e: IllegalArgumentException) {
            ResponseEntity.badRequest().body(
                ApiResponse.error(
                    code = ErrorCode.INVALID_REQUEST,
                    message = e.message ?: "잘못된 사용자 데이터입니다",
                    path = httpRequest.requestURI
                )
            )
        } catch (e: Exception) {
            ResponseEntity.badRequest().body(
                ApiResponse.error(
                    code = ErrorCode.INTERNAL_SERVER_ERROR,
                    message = "사용자 데이터 수집 중 오류가 발생했습니다",
                    path = httpRequest.requestURI
                )
            )
        }
    }

    @PostMapping("/internal/ingest/place")
    @Operation(
        summary = "배치 장소 데이터 내부 수집",
        description = "Naver + Google API 배치에서 수집한 통합 장소 데이터를 처리하고 키워드 추출을 수행합니다."
    )
    fun ingestPlaceFromBatch(
        @Parameter(description = "통합 장소 데이터 배열", required = true)
        @Valid @RequestBody placeDataList: List<InternalPlaceIngestRequest>,
        httpRequest: HttpServletRequest
    ): ResponseEntity<ApiResponse<InternalPlaceIngestResponse>> {
        return try {
            val response = batchService.ingestPlacesFromBatch(placeDataList)
            ResponseEntity.ok(ApiResponse.success(response))
        } catch (e: IllegalArgumentException) {
            ResponseEntity.badRequest().body(
                ApiResponse.error(
                    code = ErrorCode.INVALID_REQUEST,
                    message = e.message ?: "잘못된 장소 데이터입니다",
                    path = httpRequest.requestURI
                )
            )
        } catch (e: Exception) {
            ResponseEntity.badRequest().body(
                ApiResponse.error(
                    code = ErrorCode.INTERNAL_SERVER_ERROR,
                    message = "장소 데이터 수집 중 오류가 발생했습니다: ${e.message}",
                    path = httpRequest.requestURI
                )
            )
        }
    }
}

// DTOs for batch processing
data class BatchPlaceRequest(
    val externalId: String,
    val name: String,
    val description: String?,
    val category: String?,
    val address: String?,
    val latitude: Double?,
    val longitude: Double?,
    val phone: String?,
    val website: String?,
    val rating: Double?,
    val tags: List<String>?,
    val hours: String?,
    val externalCreatedAt: LocalDateTime,
    val externalUpdatedAt: LocalDateTime
)

data class BatchUserRequest(
    val externalId: String,
    val name: String,
    val email: String,
    val phone: String?,
    val department: String?,
    val status: String,
    val externalCreatedAt: LocalDateTime,
    val externalUpdatedAt: LocalDateTime
)

data class BatchPlaceResponse(
    val processedCount: Int,
    val insertedCount: Int,
    val updatedCount: Int,
    val skippedCount: Int,
    val errorCount: Int,
    val errors: List<String>
)

data class BatchUserResponse(
    val processedCount: Int,
    val insertedCount: Int,
    val updatedCount: Int,
    val skippedCount: Int,
    val errorCount: Int,
    val errors: List<String>
)

// New DTOs for internal place ingestion from batch
data class InternalPlaceIngestRequest(
    val naverPlaceId: String,
    val googlePlaceId: String?,
    val name: String,
    val description: String,
    val category: String,
    val address: String,
    val roadAddress: String?,
    val latitude: java.math.BigDecimal,
    val longitude: java.math.BigDecimal,
    val phone: String?,
    val websiteUrl: String?,
    val rating: Double?,
    val userRatingsTotal: Int?,
    val priceLevel: Int?,
    val types: List<String>,
    val openingHours: String?, // JSON string
    val imageUrl: String?,
    val sourceFlags: Map<String, Any>,
    val naverRawData: String, // JSON string
    val googleRawData: String? // JSON string
)

data class InternalPlaceIngestResponse(
    val processedCount: Int,
    val insertedCount: Int,
    val updatedCount: Int,
    val skippedCount: Int,
    val errorCount: Int,
    val keywordGeneratedCount: Int,
    val errors: List<String>
)