package com.example.ingestion.service.impl;

import com.example.ingestion.repository.PlaceRepository;
import com.example.ingestion.repository.PlaceImageRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.concurrent.CompletableFuture;

/**
 * Service for handling database initialization before batch processing
 * Ensures clean state for new batch runs according to requirements
 */
@Service
public class DatabaseInitializationService {

    private static final Logger logger = LoggerFactory.getLogger(DatabaseInitializationService.class);

    private final PlaceRepository placeRepository;
    private final PlaceImageRepository placeImageRepository;
    private final boolean enableAutoInitialization;

    public DatabaseInitializationService(
            PlaceRepository placeRepository,
            PlaceImageRepository placeImageRepository,
            @Value("${app.batch.auto-initialization:true}") boolean enableAutoInitialization
    ) {
        this.placeRepository = placeRepository;
        this.placeImageRepository = placeImageRepository;
        this.enableAutoInitialization = enableAutoInitialization;
    }

    /**
     * Initialize database by clearing existing place data
     * As required: "최초 실행 시 DB의 Place 관련 데이터를 초기화한 뒤"
     */
    @Transactional
    public CompletableFuture<DatabaseInitializationResult> initializePlaceDatabase() {
        if (!enableAutoInitialization) {
            logger.info("Database auto-initialization is disabled");
            return CompletableFuture.completedFuture(
                new DatabaseInitializationResult(true, "Auto-initialization disabled", 0, 0));
        }

        logger.info("🗃️ Starting database initialization - clearing existing place data");

        return CompletableFuture.supplyAsync(() -> {
            try {
                // 1. Count existing data
                long placesCount = placeRepository.countAllPlaces();
                long imagesCount = placeImageRepository.count();

                logger.info("📊 Found {} places and {} images to clear", placesCount, imagesCount);

                // 2. Clear place images first (due to foreign key constraint)
                placeImageRepository.deleteAllImages();
                logger.info("🖼️ Cleared {} place images", imagesCount);

                // 3. Clear places
                placeRepository.deleteAllPlaces();
                logger.info("🏢 Cleared {} places", placesCount);

                logger.info("✅ Database initialization completed successfully");

                return new DatabaseInitializationResult(
                        true,
                        "Database cleared successfully",
                        (int) placesCount,
                        (int) imagesCount
                );

            } catch (Exception e) {
                logger.error("❌ Database initialization failed", e);
                return new DatabaseInitializationResult(
                        false,
                        "Initialization failed: " + e.getMessage(),
                        0,
                        0
                );
            }
        });
    }

    /**
     * Check database readiness before starting batch processing
     */
    public CompletableFuture<Boolean> checkDatabaseReadiness() {
        return CompletableFuture.supplyAsync(() -> {
            try {
                long count = placeRepository.countAllPlaces();
                logger.debug("Database readiness check: {} places found", count);
                return true;
            } catch (Exception e) {
                logger.warn("Database readiness check failed: {}", e.getMessage());
                return false;
            }
        });
    }

    /**
     * Get current database statistics
     */
    public CompletableFuture<DatabaseStats> getDatabaseStats() {
        return CompletableFuture.supplyAsync(() -> {
            try {
                long placeCount = placeRepository.countAllPlaces();
                long imageCount = placeImageRepository.count();

                logger.debug("Database stats: {} places, {} images", placeCount, imageCount);

                return new DatabaseStats(
                        (int) placeCount,
                        0, // vectorCount - not used in direct mode
                        (int) imageCount,
                        0, // keywordCount - not used in direct mode
                        System.currentTimeMillis()
                );
            } catch (Exception e) {
                logger.warn("Failed to fetch database stats: {}", e.getMessage());
                return new DatabaseStats(0, 0, 0, 0, System.currentTimeMillis());
            }
        });
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