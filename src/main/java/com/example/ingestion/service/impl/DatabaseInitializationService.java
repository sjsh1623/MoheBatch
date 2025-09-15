package com.example.ingestion.service.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Service for handling database initialization before batch processing
 * Ensures clean state for new batch runs according to requirements
 */
@Service
public class DatabaseInitializationService {

    private static final Logger logger = LoggerFactory.getLogger(DatabaseInitializationService.class);

    private final WebClient webClient;
    private final String moheSpringBaseUrl;
    private final boolean enableAutoInitialization;

    public DatabaseInitializationService(
            WebClient webClient,
            @Value("${app.mohe-spring.base-url:http://mohe-backend:8080}") String moheSpringBaseUrl,
            @Value("${app.batch.auto-initialization:true}") boolean enableAutoInitialization
    ) {
        this.webClient = webClient;
        this.moheSpringBaseUrl = moheSpringBaseUrl;
        this.enableAutoInitialization = enableAutoInitialization;
    }

    /**
     * Initialize database by clearing existing place data
     * As required: "최초 실행 시 DB의 Place 관련 데이터를 초기화한 뒤"
     */
    public CompletableFuture<DatabaseInitializationResult> initializePlaceDatabase() {
        if (!enableAutoInitialization) {
            logger.info("Database auto-initialization is disabled");
            return CompletableFuture.completedFuture(
                new DatabaseInitializationResult(true, "Auto-initialization disabled", 0, 0));
        }

        logger.info("Starting database initialization - clearing existing place data");

        return webClient.post()
                .uri(moheSpringBaseUrl + "/api/batch/internal/initialize")
                .header("Content-Type", "application/json")
                .bodyValue(Map.of(
                        "clearPlaces", true,
                        "clearVectors", true,
                        "clearImages", true,
                        "clearKeywords", true,
                        "preserveUsers", true, // Keep user data
                        "preserveBookmarks", false, // Clear bookmarks for fresh recommendations
                        "timestamp", System.currentTimeMillis()
                ))
                .retrieve()
                .bodyToMono(DatabaseInitializationResponse.class)
                .timeout(Duration.ofMinutes(5))
                .map(response -> {
                    logger.info("Database initialization completed: {} places cleared, {} vectors cleared",
                               response.placesCleared, response.vectorsCleared);
                    return new DatabaseInitializationResult(
                            true,
                            "Database initialization successful",
                            response.placesCleared,
                            response.vectorsCleared
                    );
                })
                .onErrorResume(error -> {
                    logger.error("Database initialization failed: {}", error.getMessage(), error);
                    return Mono.just(new DatabaseInitializationResult(
                            false,
                            "Initialization failed: " + error.getMessage(),
                            0,
                            0
                    ));
                })
                .toFuture();
    }

    /**
     * Check database readiness before starting batch processing
     */
    public CompletableFuture<Boolean> checkDatabaseReadiness() {
        logger.debug("Checking database readiness");

        return webClient.get()
                .uri(moheSpringBaseUrl + "/api/batch/internal/health")
                .retrieve()
                .bodyToMono(Map.class)
                .timeout(Duration.ofSeconds(30))
                .map(response -> {
                    Object status = response.get("status");
                    boolean ready = "UP".equals(status);
                    logger.debug("Database readiness check: {}", ready ? "READY" : "NOT READY");
                    return ready;
                })
                .onErrorResume(error -> {
                    logger.warn("Database readiness check failed: {}", error.getMessage());
                    return Mono.just(false);
                })
                .toFuture();
    }

    /**
     * Get current database statistics
     */
    public CompletableFuture<DatabaseStats> getDatabaseStats() {
        logger.debug("Fetching database statistics");

        return webClient.get()
                .uri(moheSpringBaseUrl + "/api/batch/internal/stats")
                .retrieve()
                .bodyToMono(DatabaseStatsResponse.class)
                .timeout(Duration.ofSeconds(15))
                .map(response -> new DatabaseStats(
                        response.placeCount,
                        response.vectorCount,
                        response.imageCount,
                        response.keywordCount,
                        response.lastUpdated
                ))
                .onErrorResume(error -> {
                    logger.warn("Failed to fetch database stats: {}", error.getMessage());
                    return Mono.just(new DatabaseStats(0, 0, 0, 0, System.currentTimeMillis()));
                })
                .toFuture();
    }

    /**
     * Cleanup old data based on retention policy
     */
    public CompletableFuture<CleanupResult> cleanupOldData() {
        logger.info("Starting database cleanup of old data");

        Map<String, Object> cleanupRequest = Map.of(
                "maxAge", Duration.ofDays(30).toMillis(), // Remove data older than 30 days
                "minRating", 3.0, // Remove places with rating < 3.0
                "preserveBookmarked", true, // Keep bookmarked places
                "timestamp", System.currentTimeMillis()
        );

        return webClient.post()
                .uri(moheSpringBaseUrl + "/api/batch/internal/cleanup")
                .header("Content-Type", "application/json")
                .bodyValue(cleanupRequest)
                .retrieve()
                .bodyToMono(CleanupResponse.class)
                .timeout(Duration.ofMinutes(10))
                .map(response -> {
                    logger.info("Database cleanup completed: {} places removed, {} images cleaned",
                               response.placesRemoved, response.imagesCleaned);
                    return new CleanupResult(
                            true,
                            "Cleanup successful",
                            response.placesRemoved,
                            response.imagesCleaned
                    );
                })
                .onErrorResume(error -> {
                    logger.error("Database cleanup failed: {}", error.getMessage(), error);
                    return Mono.just(new CleanupResult(
                            false,
                            "Cleanup failed: " + error.getMessage(),
                            0,
                            0
                    ));
                })
                .toFuture();
    }

    // Response DTOs
    private static class DatabaseInitializationResponse {
        public boolean success;
        public String message;
        public int placesCleared;
        public int vectorsCleared;
        public int imagesCleared;
        public int keywordsCleared;
        public long timestamp;
    }

    private static class DatabaseStatsResponse {
        public long placeCount;
        public long vectorCount;
        public long imageCount;
        public long keywordCount;
        public long lastUpdated;
    }

    private static class CleanupResponse {
        public boolean success;
        public String message;
        public int placesRemoved;
        public int vectorsRemoved;
        public int imagesCleaned;
        public long timestamp;
    }

    // Result classes
    public static class DatabaseInitializationResult {
        private final boolean success;
        private final String message;
        private final int placesCleared;
        private final int vectorsCleared;

        public DatabaseInitializationResult(boolean success, String message,
                                          int placesCleared, int vectorsCleared) {
            this.success = success;
            this.message = message;
            this.placesCleared = placesCleared;
            this.vectorsCleared = vectorsCleared;
        }

        public boolean isSuccess() { return success; }
        public String getMessage() { return message; }
        public int getPlacesCleared() { return placesCleared; }
        public int getVectorsCleared() { return vectorsCleared; }

        @Override
        public String toString() {
            return String.format("DatabaseInitializationResult{success=%s, message='%s', " +
                               "placesCleared=%d, vectorsCleared=%d}",
                               success, message, placesCleared, vectorsCleared);
        }
    }

    public static class DatabaseStats {
        private final long placeCount;
        private final long vectorCount;
        private final long imageCount;
        private final long keywordCount;
        private final long lastUpdated;

        public DatabaseStats(long placeCount, long vectorCount, long imageCount,
                           long keywordCount, long lastUpdated) {
            this.placeCount = placeCount;
            this.vectorCount = vectorCount;
            this.imageCount = imageCount;
            this.keywordCount = keywordCount;
            this.lastUpdated = lastUpdated;
        }

        public long getPlaceCount() { return placeCount; }
        public long getVectorCount() { return vectorCount; }
        public long getImageCount() { return imageCount; }
        public long getKeywordCount() { return keywordCount; }
        public long getLastUpdated() { return lastUpdated; }

        @Override
        public String toString() {
            return String.format("DatabaseStats{places=%d, vectors=%d, images=%d, keywords=%d, lastUpdated=%d}",
                               placeCount, vectorCount, imageCount, keywordCount, lastUpdated);
        }
    }

    public static class CleanupResult {
        private final boolean success;
        private final String message;
        private final int placesRemoved;
        private final int imagesCleaned;

        public CleanupResult(boolean success, String message, int placesRemoved, int imagesCleaned) {
            this.success = success;
            this.message = message;
            this.placesRemoved = placesRemoved;
            this.imagesCleaned = imagesCleaned;
        }

        public boolean isSuccess() { return success; }
        public String getMessage() { return message; }
        public int getPlacesRemoved() { return placesRemoved; }
        public int getImagesCleaned() { return imagesCleaned; }

        @Override
        public String toString() {
            return String.format("CleanupResult{success=%s, message='%s', " +
                               "placesRemoved=%d, imagesCleaned=%d}",
                               success, message, placesRemoved, imagesCleaned);
        }
    }
}