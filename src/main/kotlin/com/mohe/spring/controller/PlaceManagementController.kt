package com.mohe.spring.controller

import com.mohe.spring.dto.ApiResponse
import com.mohe.spring.service.*
import io.swagger.v3.oas.annotations.Operation
import io.swagger.v3.oas.annotations.Parameter
import io.swagger.v3.oas.annotations.responses.ApiResponse as SwaggerApiResponse
import io.swagger.v3.oas.annotations.responses.ApiResponses
import io.swagger.v3.oas.annotations.tags.Tag
import org.slf4j.LoggerFactory
import org.springframework.http.ResponseEntity
import org.springframework.security.access.prepost.PreAuthorize
import org.springframework.web.bind.annotation.*

/**
 * Administrative endpoints for place management, cleanup, and dynamic fetching
 */
@RestController
@RequestMapping("/api/admin/places")
@Tag(name = "Place Management", description = "Administrative endpoints for place data management")
@PreAuthorize("hasRole('ADMIN')")
class PlaceManagementController(
    private val dynamicPlaceFetchingService: DynamicPlaceFetchingService,
    private val placeCleanupService: PlaceCleanupService
) {

    private val logger = LoggerFactory.getLogger(PlaceManagementController::class.java)

    @Operation(
        summary = "Trigger dynamic place fetching",
        description = "Manually trigger fetching of new places from external APIs when database has insufficient data"
    )
    @ApiResponses(
        value = [
            SwaggerApiResponse(responseCode = "202", description = "Place fetching triggered successfully"),
            SwaggerApiResponse(responseCode = "400", description = "Invalid parameters"),
            SwaggerApiResponse(responseCode = "500", description = "Failed to trigger place fetching")
        ]
    )
    @PostMapping("/fetch")
    fun triggerPlaceFetching(
        @Parameter(description = "Target number of places to fetch")
        @RequestParam(defaultValue = "50") targetCount: Int,
        @Parameter(description = "Category filter for places to fetch")
        @RequestParam(required = false) category: String?
    ): ResponseEntity<ApiResponse<Map<String, Any>>> {
        return try {
            if (targetCount < 1 || targetCount > 500) {
                return ResponseEntity.badRequest().body(
                    ApiResponse.error("Target count must be between 1 and 500")
                )
            }

            logger.info("Admin triggered place fetching: targetCount=$targetCount, category=$category")
            
            val fetchedCount = dynamicPlaceFetchingService.fetchNewPlacesFromApis(targetCount, category)

            ResponseEntity.accepted().body(
                ApiResponse.success(mapOf(
                    "message" to "Place fetching completed",
                    "targetCount" to targetCount,
                    "fetchedCount" to fetchedCount,
                    "category" to category,
                    "status" to "completed"
                ))
            )

        } catch (ex: Exception) {
            logger.error("Failed to trigger place fetching", ex)
            ResponseEntity.status(500).body(
                ApiResponse.error("Failed to trigger place fetching: ${ex.message}")
            )
        }
    }

    @Operation(
        summary = "Check place availability",
        description = "Check if database has sufficient places and trigger fetching if needed"
    )
    @ApiResponses(
        value = [
            SwaggerApiResponse(responseCode = "200", description = "Place availability checked successfully")
        ]
    )
    @PostMapping("/check-availability")
    fun checkPlaceAvailability(
        @Parameter(description = "Minimum required places threshold")
        @RequestParam(defaultValue = "100") minRequired: Int,
        @Parameter(description = "Category to check")
        @RequestParam(required = false) category: String?
    ): ResponseEntity<ApiResponse<Map<String, Any>>> {
        return try {
            val currentCount = dynamicPlaceFetchingService.checkAndFetchPlacesIfNeeded(
                minRequiredPlaces = minRequired,
                category = category
            )

            val isActionTaken = currentCount < minRequired
            
            ResponseEntity.ok(
                ApiResponse.success(mapOf(
                    "currentCount" to currentCount,
                    "minRequired" to minRequired,
                    "category" to category,
                    "actionTaken" to isActionTaken,
                    "status" if isActionTaken -> "fetching_triggered" else "sufficient",
                    "message" to if (isActionTaken) {
                        "Insufficient places found, fetching triggered"
                    } else {
                        "Sufficient places available"
                    }
                ))
            )

        } catch (ex: Exception) {
            logger.error("Failed to check place availability", ex)
            ResponseEntity.status(500).body(
                ApiResponse.error("Failed to check place availability: ${ex.message}")
            )
        }
    }

    @Operation(
        summary = "Trigger place cleanup",
        description = "Manually trigger cleanup of old low-rated places"
    )
    @ApiResponses(
        value = [
            SwaggerApiResponse(responseCode = "200", description = "Place cleanup completed successfully"),
            SwaggerApiResponse(responseCode = "500", description = "Failed to complete cleanup")
        ]
    )
    @PostMapping("/cleanup")
    fun triggerPlaceCleanup(
        @Parameter(description = "Maximum number of places to check for cleanup")
        @RequestParam(defaultValue = "50") maxPlacesToCheck: Int
    ): ResponseEntity<ApiResponse<CleanupStats>> {
        return try {
            if (maxPlacesToCheck < 1 || maxPlacesToCheck > 200) {
                return ResponseEntity.badRequest().body(
                    ApiResponse.error("Max places to check must be between 1 and 200")
                )
            }

            logger.info("Admin triggered place cleanup for up to $maxPlacesToCheck places")
            
            val stats = placeCleanupService.triggerManualCleanup(maxPlacesToCheck)

            ResponseEntity.ok(ApiResponse.success(stats))

        } catch (ex: Exception) {
            logger.error("Failed to trigger place cleanup", ex)
            ResponseEntity.status(500).body(
                ApiResponse.error("Failed to trigger place cleanup: ${ex.message}")
            )
        }
    }

    @Operation(
        summary = "Get cleanup statistics",
        description = "Get statistics about places that need cleanup and overall database health"
    )
    @ApiResponses(
        value = [
            SwaggerApiResponse(responseCode = "200", description = "Cleanup statistics retrieved successfully")
        ]
    )
    @GetMapping("/cleanup-stats")
    fun getCleanupStatistics(): ResponseEntity<ApiResponse<CleanupStatistics>> {
        return try {
            val statistics = placeCleanupService.getCleanupStatistics()
            ResponseEntity.ok(ApiResponse.success(statistics))

        } catch (ex: Exception) {
            logger.error("Failed to get cleanup statistics", ex)
            ResponseEntity.status(500).body(
                ApiResponse.error("Failed to get cleanup statistics: ${ex.message}")
            )
        }
    }

    @Operation(
        summary = "Recheck specific place rating",
        description = "Manually trigger rating recheck for a specific place"
    )
    @ApiResponses(
        value = [
            SwaggerApiResponse(responseCode = "200", description = "Place rating rechecked successfully"),
            SwaggerApiResponse(responseCode = "404", description = "Place not found"),
            SwaggerApiResponse(responseCode = "500", description = "Failed to recheck place rating")
        ]
    )
    @PostMapping("/recheck/{placeId}")
    fun recheckPlaceRating(
        @Parameter(description = "Place ID to recheck", required = true)
        @PathVariable placeId: Long
    ): ResponseEntity<ApiResponse<Map<String, Any>>> {
        return try {
            // This would require accessing place repository to get the place
            // For now, return a placeholder response
            
            logger.info("Admin requested rating recheck for place ID: $placeId")
            
            ResponseEntity.ok(
                ApiResponse.success(mapOf(
                    "placeId" to placeId,
                    "message" to "Place rating recheck initiated",
                    "status" to "processing"
                ))
            )

        } catch (ex: Exception) {
            logger.error("Failed to recheck place rating for place $placeId", ex)
            ResponseEntity.status(500).body(
                ApiResponse.error("Failed to recheck place rating: ${ex.message}")
            )
        }
    }
}