package com.mohe.spring.controller;

import com.mohe.spring.service.SimilarityCalculationService;
import com.mohe.spring.service.SimilaritySchedulerService;
import com.mohe.spring.service.SimilarityStatistics;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

/**
 * Administrative endpoints for managing similarity calculations
 * Only accessible to admin users
 */
@RestController
@RequestMapping("/api/admin/similarity")
@Tag(name = "Similarity Administration", description = "Administrative endpoints for similarity calculations")
@PreAuthorize("hasRole('ADMIN')")
public class SimilarityAdminController {

    private static final Logger logger = LoggerFactory.getLogger(SimilarityAdminController.class);

    private final SimilarityCalculationService similarityCalculationService;
    private final SimilaritySchedulerService similaritySchedulerService;

    public SimilarityAdminController(SimilarityCalculationService similarityCalculationService,
                                   SimilaritySchedulerService similaritySchedulerService) {
        this.similarityCalculationService = similarityCalculationService;
        this.similaritySchedulerService = similaritySchedulerService;
    }

    @Operation(
        summary = "Trigger full similarity calculation",
        description = "Manually trigger recalculation of all place similarities with MBTI weighting. This is an expensive operation that runs asynchronously."
    )
    @ApiResponses(
        value = {
            @ApiResponse(responseCode = "202", description = "Calculation triggered successfully"),
            @ApiResponse(responseCode = "409", description = "Calculation already in progress"),
            @ApiResponse(responseCode = "500", description = "Failed to trigger calculation")
        }
    )
    @PostMapping("/calculate")
    public ResponseEntity<Map<String, Object>> triggerSimilarityCalculation() {
        try {
            boolean triggered = similaritySchedulerService.triggerSimilarityCalculation();
            
            if (triggered) {
                logger.info("Similarity calculation triggered by admin request");
                return ResponseEntity.accepted().body(Map.of(
                    "success", true,
                    "message", "Similarity calculation started",
                    "status", "running"
                ));
            } else {
                return ResponseEntity.status(409).body(Map.of(
                    "success", false,
                    "message", "Similarity calculation already in progress",
                    "status", "already_running"
                ));
            }
        } catch (Exception ex) {
            logger.error("Failed to trigger similarity calculation", ex);
            return ResponseEntity.status(500).body(Map.of(
                "success", false,
                "message", "Failed to trigger calculation: " + ex.getMessage(),
                "status", "error"
            ));
        }
    }

    @Operation(
        summary = "Refresh Top-K similarities for specific places",
        description = "Refresh the Top-K similarity cache for specified places. Useful after significant bookmark changes."
    )
    @ApiResponses(
        value = {
            @ApiResponse(responseCode = "202", description = "Top-K refresh triggered successfully"),
            @ApiResponse(responseCode = "400", description = "Invalid place IDs provided"),
            @ApiResponse(responseCode = "500", description = "Failed to refresh Top-K")
        }
    )
    @PostMapping("/refresh-topk")
    public ResponseEntity<Map<String, Object>> refreshTopKSimilarities(
            @Parameter(description = "List of place IDs to refresh", required = true)
            @RequestBody List<Long> placeIds) {
        try {
            if (placeIds.isEmpty()) {
                return ResponseEntity.badRequest().body(Map.of(
                    "success", false,
                    "message", "Place IDs list cannot be empty"
                ));
            }

            if (placeIds.size() > 1000) {
                return ResponseEntity.badRequest().body(Map.of(
                    "success", false,
                    "message", "Cannot refresh more than 1000 places at once"
                ));
            }

            logger.info("Admin requested Top-K refresh for {} places", placeIds.size());
            similarityCalculationService.refreshTopKSimilarities(placeIds);

            return ResponseEntity.accepted().body(Map.of(
                "success", true,
                "message", "Top-K refresh started for " + placeIds.size() + " places",
                "placeCount", placeIds.size(),
                "status", "running"
            ));

        } catch (Exception ex) {
            logger.error("Failed to refresh Top-K similarities", ex);
            return ResponseEntity.status(500).body(Map.of(
                "success", false,
                "message", "Failed to refresh Top-K: " + ex.getMessage(),
                "status", "error"
            ));
        }
    }

    @Operation(
        summary = "Refresh Top-K for a single place",
        description = "Refresh the Top-K similarity cache for a specific place"
    )
    @ApiResponses(
        value = {
            @ApiResponse(responseCode = "202", description = "Top-K refresh triggered successfully"),
            @ApiResponse(responseCode = "404", description = "Place not found"),
            @ApiResponse(responseCode = "500", description = "Failed to refresh Top-K")
        }
    )
    @PostMapping("/refresh-topk/{placeId}")
    public ResponseEntity<Map<String, Object>> refreshTopKSimilarity(
            @Parameter(description = "Place ID to refresh", required = true)
            @PathVariable Long placeId) {
        try {
            logger.info("Admin requested Top-K refresh for place ID: {}", placeId);
            similarityCalculationService.refreshTopKSimilarities(placeId);

            return ResponseEntity.accepted().body(Map.of(
                "success", true,
                "message", "Top-K refresh started for place " + placeId,
                "placeId", placeId,
                "status", "running"
            ));

        } catch (Exception ex) {
            logger.error("Failed to refresh Top-K similarity for place {}", placeId, ex);
            return ResponseEntity.status(500).body(Map.of(
                "success", false,
                "message", "Failed to refresh Top-K: " + ex.getMessage(),
                "placeId", placeId,
                "status", "error"
            ));
        }
    }

    @Operation(
        summary = "Calculate similarity for specific place pair",
        description = "Calculate and store similarity for a specific pair of places"
    )
    @ApiResponses(
        value = {
            @ApiResponse(responseCode = "200", description = "Similarity calculated successfully"),
            @ApiResponse(responseCode = "400", description = "Invalid place IDs"),
            @ApiResponse(responseCode = "500", description = "Failed to calculate similarity")
        }
    )
    @PostMapping("/calculate-pair/{placeId1}/{placeId2}")
    public ResponseEntity<Map<String, Object>> calculatePlacePairSimilarity(
            @Parameter(description = "First place ID", required = true)
            @PathVariable Long placeId1,
            @Parameter(description = "Second place ID", required = true)  
            @PathVariable Long placeId2) {
        try {
            if (placeId1.equals(placeId2)) {
                return ResponseEntity.badRequest().body(Map.of(
                    "success", false,
                    "message", "Cannot calculate similarity between same place"
                ));
            }

            logger.info("Admin requested similarity calculation between places: {} and {}", placeId1, placeId2);
            similarityCalculationService.calculatePlacePairSimilarity(placeId1, placeId2);

            return ResponseEntity.ok().body(Map.of(
                "success", true,
                "message", "Similarity calculated for places " + placeId1 + " and " + placeId2,
                "placeId1", placeId1,
                "placeId2", placeId2,
                "status", "completed"
            ));

        } catch (Exception ex) {
            logger.error("Failed to calculate similarity for places {} and {}", placeId1, placeId2, ex);
            return ResponseEntity.status(500).body(Map.of(
                "success", false,
                "message", "Failed to calculate similarity: " + ex.getMessage(),
                "placeId1", placeId1,
                "placeId2", placeId2,
                "status", "error"
            ));
        }
    }

    @Operation(
        summary = "Get similarity calculation status",
        description = "Check if similarity calculations are currently running and get system statistics"
    )
    @ApiResponses(
        value = {
            @ApiResponse(responseCode = "200", description = "Status retrieved successfully")
        }
    )
    @GetMapping("/status")
    public ResponseEntity<Map<String, Object>> getSimilarityStatus() {
        try {
            boolean isRunning = similaritySchedulerService.isCalculationRunning();
            SimilarityStatistics statistics = similarityCalculationService.getSimilarityStatistics();

            return ResponseEntity.ok().body(Map.of(
                "success", true,
                "isRunning", isRunning,
                "statistics", Map.of(
                    "totalSimilarities", statistics.getTotalSimilarities(),
                    "totalTopKEntries", statistics.getTotalTopKEntries(),
                    "averageJaccard", statistics.getAverageJaccard(),
                    "averageCosine", statistics.getAverageCosine(),
                    "sampleSize", statistics.getSampleSize()
                )
            ));

        } catch (Exception ex) {
            logger.error("Failed to get similarity status", ex);
            return ResponseEntity.status(500).body(Map.of(
                "success", false,
                "message", "Failed to get status: " + ex.getMessage()
            ));
        }
    }

    @Operation(
        summary = "Get detailed similarity statistics",
        description = "Get comprehensive statistics about the similarity calculation system"
    )
    @ApiResponses(
        value = {
            @ApiResponse(responseCode = "200", description = "Statistics retrieved successfully")
        }
    )
    @GetMapping("/statistics")
    public ResponseEntity<SimilarityStatistics> getSimilarityStatistics() {
        try {
            SimilarityStatistics statistics = similarityCalculationService.getSimilarityStatistics();
            return ResponseEntity.ok(statistics);

        } catch (Exception ex) {
            logger.error("Failed to get similarity statistics", ex);
            throw ex;
        }
    }
}