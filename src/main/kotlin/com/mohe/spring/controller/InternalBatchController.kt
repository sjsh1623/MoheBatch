package com.mohe.spring.controller

import com.mohe.spring.dto.ApiResponse
import com.mohe.spring.dto.ErrorCode
import com.mohe.spring.service.InternalPlaceIngestService
import io.swagger.v3.oas.annotations.Operation
import io.swagger.v3.oas.annotations.responses.ApiResponse as SwaggerApiResponse
import io.swagger.v3.oas.annotations.responses.ApiResponses
import io.swagger.v3.oas.annotations.tags.Tag
import jakarta.servlet.http.HttpServletRequest
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.*
import java.math.BigDecimal

/**
 * Internal endpoints for batch processing
 * These endpoints are used by the MoheBatch service to ingest places
 */
@RestController
@RequestMapping("/api/batch/internal")
@Tag(name = "Internal Batch Processing", description = "Internal endpoints used by MoheBatch service")
class InternalBatchController(
    private val internalPlaceIngestService: InternalPlaceIngestService
) {

    @PostMapping("/ingest/place")
    @Operation(
        summary = "Internal place ingestion endpoint",
        description = "Used by MoheBatch service to ingest processed places with image fetching"
    )
    @ApiResponses(
        value = [
            SwaggerApiResponse(responseCode = "200", description = "Places ingested successfully"),
            SwaggerApiResponse(responseCode = "400", description = "Invalid request data")
        ]
    )
    fun ingestPlaces(
        @RequestBody requests: List<InternalPlaceIngestRequest>,
        httpRequest: HttpServletRequest
    ): ResponseEntity<ApiResponse<InternalPlaceIngestResponse>> {
        return try {
            val response = internalPlaceIngestService.ingestPlaces(requests)
            ResponseEntity.ok(ApiResponse.success(response))
        } catch (e: Exception) {
            ResponseEntity.badRequest().body(
                ApiResponse.error(
                    code = ErrorCode.INTERNAL_SERVER_ERROR,
                    message = e.message ?: "Failed to ingest places",
                    path = httpRequest.requestURI
                )
            )
        }
    }
}

/**
 * Request DTOs for internal batch communication
 */
data class InternalPlaceIngestRequest(
    val naverPlaceId: String,
    val googlePlaceId: String?,
    val name: String,
    val description: String,
    val category: String,
    val address: String,
    val roadAddress: String?,
    val latitude: BigDecimal,
    val longitude: BigDecimal,
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
    val googleRawData: String?, // JSON string
    val keywordVector: List<Double> = emptyList() // Embedding vector from Ollama
)

/**
 * Response DTOs for internal batch communication
 */
data class InternalPlaceIngestResponse(
    val processedCount: Int,
    val insertedCount: Int,
    val updatedCount: Int,
    val skippedCount: Int,
    val errorCount: Int,
    val keywordGeneratedCount: Int,
    val imagesFetchedCount: Int, // New: count of images fetched
    val errors: List<String>
)