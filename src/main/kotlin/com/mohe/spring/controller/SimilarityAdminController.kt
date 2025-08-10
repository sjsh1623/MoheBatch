package com.mohe.spring.controller

import com.mohe.spring.service.SimilarityCalculationService
import com.mohe.spring.service.SimilaritySchedulerService
import com.mohe.spring.service.SimilarityStatistics
import io.swagger.v3.oas.annotations.Operation
import io.swagger.v3.oas.annotations.Parameter
import io.swagger.v3.oas.annotations.responses.ApiResponse
import io.swagger.v3.oas.annotations.responses.ApiResponses
import io.swagger.v3.oas.annotations.tags.Tag
import org.slf4j.LoggerFactory
import org.springframework.http.ResponseEntity
import org.springframework.security.access.prepost.PreAuthorize
import org.springframework.web.bind.annotation.*

/**
 * Administrative endpoints for managing similarity calculations
 * Only accessible to admin users
 */
@RestController
@RequestMapping("/api/admin/similarity")
@Tag(name = "Similarity Administration", description = "Administrative endpoints for similarity calculations")
@PreAuthorize("hasRole('ADMIN')")
class SimilarityAdminController(
    private val similarityCalculationService: SimilarityCalculationService,
    private val similaritySchedulerService: SimilaritySchedulerService
) {

    private val logger = LoggerFactory.getLogger(SimilarityAdminController::class.java)

    @Operation(
        summary = "Trigger full similarity calculation",
        description = "Manually trigger recalculation of all place similarities with MBTI weighting. This is an expensive operation that runs asynchronously."
    )
    @ApiResponses(
        value = [
            ApiResponse(responseCode = "202", description = "Calculation triggered successfully"),
            ApiResponse(responseCode = "409", description = "Calculation already in progress"),
            ApiResponse(responseCode = "500", description = "Failed to trigger calculation")
        ]
    )
    @PostMapping("/calculate")
    fun triggerSimilarityCalculation(): ResponseEntity<Map<String, Any>> {
        return try {
            val triggered = similaritySchedulerService.triggerSimilarityCalculation()
            
            if (triggered) {
                logger.info("Similarity calculation triggered by admin request")
                ResponseEntity.accepted().body(mapOf(
                    "success" to true,
                    "message" to "Similarity calculation started",
                    "status" to "running"
                ))
            } else {
                ResponseEntity.status(409).body(mapOf(
                    "success" to false,
                    "message" to "Similarity calculation already in progress",
                    "status" to "already_running"
                ))
            }
        } catch (ex: Exception) {
            logger.error("Failed to trigger similarity calculation", ex)
            ResponseEntity.status(500).body(mapOf(
                "success" to false,
                "message" to "Failed to trigger calculation: ${ex.message}",
                "status" to "error"
            ))
        }
    }

    @Operation(
        summary = "Refresh Top-K similarities for specific places",
        description = "Refresh the Top-K similarity cache for specified places. Useful after significant bookmark changes."
    )
    @ApiResponses(
        value = [
            ApiResponse(responseCode = "202", description = "Top-K refresh triggered successfully"),
            ApiResponse(responseCode = "400", description = "Invalid place IDs provided"),
            ApiResponse(responseCode = "500", description = "Failed to refresh Top-K")
        ]
    )
    @PostMapping("/refresh-topk")
    fun refreshTopKSimilarities(
        @Parameter(description = "List of place IDs to refresh", required = true)
        @RequestBody placeIds: List<Long>
    ): ResponseEntity<Map<String, Any>> {
        return try {
            if (placeIds.isEmpty()) {
                return ResponseEntity.badRequest().body(mapOf(
                    "success" to false,
                    "message" to "Place IDs list cannot be empty"
                ))
            }

            if (placeIds.size > 1000) {
                return ResponseEntity.badRequest().body(mapOf(
                    "success" to false,
                    "message" to "Cannot refresh more than 1000 places at once"
                ))
            }

            logger.info("Admin requested Top-K refresh for ${placeIds.size} places")
            similarityCalculationService.refreshTopKSimilarities(placeIds)

            ResponseEntity.accepted().body(mapOf(
                "success" to true,
                "message" to "Top-K refresh started for ${placeIds.size} places",
                "placeCount" to placeIds.size,
                "status" to "running"
            ))

        } catch (ex: Exception) {
            logger.error("Failed to refresh Top-K similarities", ex)
            ResponseEntity.status(500).body(mapOf(
                "success" to false,
                "message" to "Failed to refresh Top-K: ${ex.message}",
                "status" to "error"
            ))
        }
    }

    @Operation(
        summary = "Refresh Top-K for a single place",
        description = "Refresh the Top-K similarity cache for a specific place"
    )
    @ApiResponses(
        value = [
            ApiResponse(responseCode = "202", description = "Top-K refresh triggered successfully"),
            ApiResponse(responseCode = "404", description = "Place not found"),
            ApiResponse(responseCode = "500", description = "Failed to refresh Top-K")
        ]
    )
    @PostMapping("/refresh-topk/{placeId}")
    fun refreshTopKSimilarity(
        @Parameter(description = "Place ID to refresh", required = true)
        @PathVariable placeId: Long
    ): ResponseEntity<Map<String, Any>> {
        return try {
            logger.info("Admin requested Top-K refresh for place ID: $placeId")
            similarityCalculationService.refreshTopKSimilarities(placeId)

            ResponseEntity.accepted().body(mapOf(
                "success" to true,
                "message" to "Top-K refresh started for place $placeId",
                "placeId" to placeId,
                "status" to "running"
            ))

        } catch (ex: Exception) {
            logger.error("Failed to refresh Top-K similarity for place $placeId", ex)
            ResponseEntity.status(500).body(mapOf(
                "success" to false,
                "message" to "Failed to refresh Top-K: ${ex.message}",
                "placeId" to placeId,
                "status" to "error"
            ))
        }
    }

    @Operation(
        summary = "Calculate similarity for specific place pair",
        description = "Calculate and store similarity for a specific pair of places"
    )
    @ApiResponses(
        value = [
            ApiResponse(responseCode = "200", description = "Similarity calculated successfully"),
            ApiResponse(responseCode = "400", description = "Invalid place IDs"),
            ApiResponse(responseCode = "500", description = "Failed to calculate similarity")
        ]
    )
    @PostMapping("/calculate-pair/{placeId1}/{placeId2}")
    fun calculatePlacePairSimilarity(
        @Parameter(description = "First place ID", required = true)
        @PathVariable placeId1: Long,
        @Parameter(description = "Second place ID", required = true)  
        @PathVariable placeId2: Long
    ): ResponseEntity<Map<String, Any>> {
        return try {
            if (placeId1 == placeId2) {
                return ResponseEntity.badRequest().body(mapOf(
                    "success" to false,
                    "message" to "Cannot calculate similarity between same place"
                ))
            }

            logger.info("Admin requested similarity calculation between places: $placeId1 and $placeId2")
            similarityCalculationService.calculatePlacePairSimilarity(placeId1, placeId2)

            ResponseEntity.ok().body(mapOf(
                "success" to true,
                "message" to "Similarity calculated for places $placeId1 and $placeId2",
                "placeId1" to placeId1,
                "placeId2" to placeId2,
                "status" to "completed"
            ))

        } catch (ex: Exception) {
            logger.error("Failed to calculate similarity for places $placeId1 and $placeId2", ex)
            ResponseEntity.status(500).body(mapOf(
                "success" to false,
                "message" to "Failed to calculate similarity: ${ex.message}",
                "placeId1" to placeId1,
                "placeId2" to placeId2,
                "status" to "error"
            ))
        }
    }

    @Operation(
        summary = "Get similarity calculation status",
        description = "Check if similarity calculations are currently running and get system statistics"
    )
    @ApiResponses(
        value = [
            ApiResponse(responseCode = "200", description = "Status retrieved successfully")
        ]
    )
    @GetMapping("/status")
    fun getSimilarityStatus(): ResponseEntity<Map<String, Any>> {
        return try {
            val isRunning = similaritySchedulerService.isCalculationRunning()
            val statistics = similarityCalculationService.getSimilarityStatistics()

            ResponseEntity.ok().body(mapOf(
                "success" to true,
                "isRunning" to isRunning,
                "statistics" to mapOf(
                    "totalSimilarities" to statistics.totalSimilarities,
                    "totalTopKEntries" to statistics.totalTopKEntries,
                    "averageJaccard" to statistics.averageJaccard,
                    "averageCosine" to statistics.averageCosine,
                    "sampleSize" to statistics.sampleSize
                )
            ))

        } catch (ex: Exception) {
            logger.error("Failed to get similarity status", ex)
            ResponseEntity.status(500).body(mapOf(
                "success" to false,
                "message" to "Failed to get status: ${ex.message}"
            ))
        }
    }

    @Operation(
        summary = "Get detailed similarity statistics",
        description = "Get comprehensive statistics about the similarity calculation system"
    )
    @ApiResponses(
        value = [
            ApiResponse(responseCode = "200", description = "Statistics retrieved successfully")
        ]
    )
    @GetMapping("/statistics")
    fun getSimilarityStatistics(): ResponseEntity<SimilarityStatistics> {
        return try {
            val statistics = similarityCalculationService.getSimilarityStatistics()
            ResponseEntity.ok(statistics)

        } catch (ex: Exception) {
            logger.error("Failed to get similarity statistics", ex)
            throw ex
        }
    }
}