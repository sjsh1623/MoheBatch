package com.example.ingestion.controller;

import com.example.ingestion.service.PlaceFilterService;
import com.example.ingestion.service.impl.DatabaseInitializationService;
import com.example.ingestion.service.impl.OptimizedContinuousBatchService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * REST controller for optimized batch management
 * Provides endpoints for controlling the optimized batch processing
 */
@RestController
@RequestMapping("/api/batch/optimized")
public class OptimizedBatchController {

    private static final Logger logger = LoggerFactory.getLogger(OptimizedBatchController.class);

    @Autowired(required = false)
    private OptimizedContinuousBatchService continuousBatchService;

    private final DatabaseInitializationService databaseInitializationService;
    private final PlaceFilterService placeFilterService;

    public OptimizedBatchController(
            DatabaseInitializationService databaseInitializationService,
            PlaceFilterService placeFilterService
    ) {
        this.databaseInitializationService = databaseInitializationService;
        this.placeFilterService = placeFilterService;
    }

    /**
     * Start continuous batch processing
     */
    @PostMapping("/start")
    public ResponseEntity<Map<String, Object>> startContinuousProcessing() {
        logger.info("🚀 Received request to start continuous batch processing");

        try {
            if (continuousBatchService == null) {
                return ResponseEntity.badRequest().body(Map.of(
                        "success", false,
                        "message", "Continuous batch processing is disabled. Enable with app.batch.continuous.enabled=true",
                        "timestamp", System.currentTimeMillis()
                ));
            }

            continuousBatchService.startContinuousProcessing();

            return ResponseEntity.ok(Map.of(
                    "success", true,
                    "message", "Continuous batch processing started successfully",
                    "mode", "optimized_infinite_loop",
                    "timestamp", System.currentTimeMillis()
            ));

        } catch (Exception e) {
            logger.error("❌ Failed to start continuous batch processing", e);
            return ResponseEntity.internalServerError().body(Map.of(
                    "success", false,
                    "message", "Failed to start continuous processing: " + e.getMessage(),
                    "timestamp", System.currentTimeMillis()
            ));
        }
    }

    /**
     * Stop continuous batch processing
     */
    @PostMapping("/stop")
    public ResponseEntity<Map<String, Object>> stopContinuousProcessing() {
        logger.info("🛑 Received request to stop continuous batch processing");

        try {
            if (continuousBatchService == null) {
                return ResponseEntity.badRequest().body(Map.of(
                        "success", false,
                        "message", "Continuous batch processing is disabled. Enable with app.batch.continuous.enabled=true",
                        "timestamp", System.currentTimeMillis()
                ));
            }

            continuousBatchService.stopContinuousProcessing();

            return ResponseEntity.ok(Map.of(
                    "success", true,
                    "message", "Continuous batch processing stopped successfully",
                    "timestamp", System.currentTimeMillis()
            ));

        } catch (Exception e) {
            logger.error("❌ Failed to stop continuous batch processing", e);
            return ResponseEntity.internalServerError().body(Map.of(
                    "success", false,
                    "message", "Failed to stop continuous processing: " + e.getMessage(),
                    "timestamp", System.currentTimeMillis()
            ));
        }
    }

    /**
     * Get current batch processing status
     */
    @GetMapping("/status")
    public ResponseEntity<Map<String, Object>> getProcessingStatus() {
        try {
            if (continuousBatchService == null) {
                return ResponseEntity.ok(Map.of(
                        "success", true,
                        "status", Map.of(
                                "running", false,
                                "enabled", false,
                                "message", "Continuous batch processing is disabled"
                        ),
                        "timestamp", System.currentTimeMillis()
                ));
            }

            OptimizedContinuousBatchService.ServiceStatus status = continuousBatchService.getServiceStatus();

            return ResponseEntity.ok(Map.of(
                    "success", true,
                    "status", Map.of(
                            "running", status.isRunning(),
                            "totalBatches", status.getTotalBatches(),
                            "successfulBatches", status.getSuccessfulBatches(),
                            "failedBatches", status.getFailedBatches(),
                            "successRate", Math.round(status.getSuccessRate() * 100.0) / 100.0,
                            "uptimeMs", status.getUptimeMs(),
                            "uptimeHours", Math.round(status.getUptimeMs() / 3600000.0 * 100.0) / 100.0,
                            "currentBackoffMs", status.getCurrentBackoffMs()
                    ),
                    "timestamp", System.currentTimeMillis()
            ));

        } catch (Exception e) {
            logger.error("❌ Failed to get processing status", e);
            return ResponseEntity.internalServerError().body(Map.of(
                    "success", false,
                    "message", "Failed to get status: " + e.getMessage(),
                    "timestamp", System.currentTimeMillis()
            ));
        }
    }

    /**
     * Initialize database (clear existing place data)
     */
    @PostMapping("/initialize")
    public ResponseEntity<Map<String, Object>> initializeDatabase() {
        logger.info("🗃️ Received request to initialize database");

        try {
            CompletableFuture<DatabaseInitializationService.DatabaseInitializationResult> future =
                    databaseInitializationService.initializePlaceDatabase();

            DatabaseInitializationService.DatabaseInitializationResult result = future.get();

            if (result.isSuccess()) {
                return ResponseEntity.ok(Map.of(
                        "success", true,
                        "message", result.getMessage(),
                        "placesCleared", result.getPlacesCleared(),
                        "vectorsCleared", result.getVectorsCleared(),
                        "timestamp", System.currentTimeMillis()
                ));
            } else {
                return ResponseEntity.internalServerError().body(Map.of(
                        "success", false,
                        "message", result.getMessage(),
                        "timestamp", System.currentTimeMillis()
                ));
            }

        } catch (Exception e) {
            logger.error("❌ Failed to initialize database", e);
            return ResponseEntity.internalServerError().body(Map.of(
                    "success", false,
                    "message", "Failed to initialize database: " + e.getMessage(),
                    "timestamp", System.currentTimeMillis()
            ));
        }
    }

    /**
     * Get database statistics
     */
    @GetMapping("/database/stats")
    public ResponseEntity<Map<String, Object>> getDatabaseStats() {
        try {
            CompletableFuture<DatabaseInitializationService.DatabaseStats> future =
                    databaseInitializationService.getDatabaseStats();

            DatabaseInitializationService.DatabaseStats stats = future.get();

            return ResponseEntity.ok(Map.of(
                    "success", true,
                    "stats", Map.of(
                            "placeCount", stats.getPlaceCount(),
                            "vectorCount", stats.getVectorCount(),
                            "imageCount", stats.getImageCount(),
                            "keywordCount", stats.getKeywordCount(),
                            "lastUpdated", stats.getLastUpdated()
                    ),
                    "timestamp", System.currentTimeMillis()
            ));

        } catch (Exception e) {
            logger.error("❌ Failed to get database stats", e);
            return ResponseEntity.internalServerError().body(Map.of(
                    "success", false,
                    "message", "Failed to get database stats: " + e.getMessage(),
                    "timestamp", System.currentTimeMillis()
            ));
        }
    }

    /**
     * Get filtering configuration and statistics
     */
    @GetMapping("/filtering/config")
    public ResponseEntity<Map<String, Object>> getFilteringConfig() {
        try {
            return ResponseEntity.ok(Map.of(
                    "success", true,
                    "filtering", Map.of(
                            "enabled", true,
                            "excludedKeywords", java.util.List.of(
                                    "클럽", "나이트", "성인", "룸살롱", "마트", "편의점",
                                    "대형마트", "할인점", "백화점", "아울렛", "쇼핑센터"
                            ),
                            "excludedCategories", java.util.List.of(
                                    "convenience_store", "supermarket", "department_store",
                                    "night_club", "bar", "adult_entertainment"
                            ),
                            "description", "Comprehensive place filtering as per business requirements"
                    ),
                    "timestamp", System.currentTimeMillis()
            ));

        } catch (Exception e) {
            logger.error("❌ Failed to get filtering config", e);
            return ResponseEntity.internalServerError().body(Map.of(
                    "success", false,
                    "message", "Failed to get filtering config: " + e.getMessage(),
                    "timestamp", System.currentTimeMillis()
            ));
        }
    }

    /**
     * Cleanup old data
     */
    @PostMapping("/cleanup")
    public ResponseEntity<Map<String, Object>> cleanupOldData() {
        logger.info("🧹 Received request to cleanup old data");

        try {
            CompletableFuture<DatabaseInitializationService.CleanupResult> future =
                    databaseInitializationService.cleanupOldData();

            DatabaseInitializationService.CleanupResult result = future.get();

            if (result.isSuccess()) {
                return ResponseEntity.ok(Map.of(
                        "success", true,
                        "message", result.getMessage(),
                        "placesRemoved", result.getPlacesRemoved(),
                        "imagesCleaned", result.getImagesCleaned(),
                        "timestamp", System.currentTimeMillis()
                ));
            } else {
                return ResponseEntity.internalServerError().body(Map.of(
                        "success", false,
                        "message", result.getMessage(),
                        "timestamp", System.currentTimeMillis()
                ));
            }

        } catch (Exception e) {
            logger.error("❌ Failed to cleanup old data", e);
            return ResponseEntity.internalServerError().body(Map.of(
                    "success", false,
                    "message", "Failed to cleanup: " + e.getMessage(),
                    "timestamp", System.currentTimeMillis()
            ));
        }
    }
}