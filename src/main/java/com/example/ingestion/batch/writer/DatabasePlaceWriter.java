package com.example.ingestion.batch.writer;

import com.example.ingestion.dto.ProcessedPlaceJava;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Database writer that saves places to PostgreSQL database
 */
@Component
public class DatabasePlaceWriter implements ItemWriter<ProcessedPlaceJava> {

    private static final Logger logger = LoggerFactory.getLogger(DatabasePlaceWriter.class);

    @Autowired
    private JdbcTemplate jdbcTemplate;

    private final AtomicInteger totalWritten = new AtomicInteger(0);
    private final AtomicInteger chunkCount = new AtomicInteger(0);

    @Override
    public void write(Chunk<? extends ProcessedPlaceJava> chunk) throws Exception {
        if (chunk.isEmpty()) {
            logger.debug("📝 Empty chunk received, skipping write");
            return;
        }

        int chunkNum = chunkCount.incrementAndGet();
        int chunkSize = chunk.size();
        int totalSoFar = totalWritten.addAndGet(chunkSize);

        logger.info("📝 Writing chunk #{}: {} places to database (total written: {})", chunkNum, chunkSize, totalSoFar);

        try {
            // Insert places into database
            for (ProcessedPlaceJava place : chunk.getItems()) {
                insertPlaceToDatabase(place);
            }

            logger.info("✅ Chunk #{} written successfully to database", chunkNum);

        } catch (Exception e) {
            logger.error("❌ Failed to write chunk #{} to database", chunkNum, e);
            throw e; // Re-throw to trigger batch failure handling
        }
    }

    private void insertPlaceToDatabase(ProcessedPlaceJava place) {
        try {
            // Check if place already exists by naver_place_id
            String checkSql = "SELECT COUNT(*) FROM places WHERE naver_place_id = ?";
            Integer count = jdbcTemplate.queryForObject(checkSql, Integer.class, place.getNaverPlaceId());

            if (count != null && count > 0) {
                logger.debug("  🔄 Updating existing place: {}", place.getName());
                updateExistingPlace(place);
            } else {
                logger.debug("  ➕ Inserting new place: {}", place.getName());
                insertNewPlace(place);
            }

        } catch (Exception e) {
            logger.error("❌ Failed to save place: {} - {}", place.getName(), e.getMessage());
            throw new RuntimeException("Failed to save place: " + place.getName(), e);
        }
    }

    private void insertNewPlace(ProcessedPlaceJava place) {
        String sql = """
            INSERT INTO places (
                name, description, latitude, longitude, category, rating,
                address, road_address, phone, naver_place_id, google_place_id,
                opening_hours, tags, amenities, images, gallery,
                review_count, user_ratings_total, price_level,
                source_flags, keyword_vector, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?::jsonb, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """;

        Timestamp now = Timestamp.from(Instant.now());

        jdbcTemplate.update(sql,
            place.getName(),
            place.getDescription(),
            place.getLatitude(),
            place.getLongitude(),
            place.getCategory(),
            place.getRating(),
            place.getAddress(),
            place.getRoadAddress(),
            place.getPhone(),
            place.getNaverPlaceId(),
            place.getGooglePlaceId(),
            place.getOpeningHours() != null ? place.getOpeningHours().toString() : "{}",
            place.getTags() != null ? place.getTags().toArray(new String[0]) : new String[0],
            place.getAmenities() != null ? place.getAmenities().toArray(new String[0]) : new String[0],
            place.getImages() != null ? place.getImages().toArray(new String[0]) : new String[0],
            place.getGallery() != null ? place.getGallery().toArray(new String[0]) : new String[0],
            place.getReviewCount(),
            place.getUserRatingsTotal(),
            place.getPriceLevel(),
            place.getSourceFlags() != null ? place.getSourceFlags().toString() : "{}",
            place.getKeywordVector() != null ? place.getKeywordVector().toString() : null,
            now,
            now
        );

        logger.debug("  ✅ Inserted place: {} (ID: {})", place.getName(), place.getNaverPlaceId());
    }

    private void updateExistingPlace(ProcessedPlaceJava place) {
        String sql = """
            UPDATE places SET
                name = ?, description = ?, latitude = ?, longitude = ?, category = ?,
                rating = ?, address = ?, road_address = ?, phone = ?,
                google_place_id = ?, opening_hours = ?::jsonb, tags = ?, amenities = ?,
                images = ?, gallery = ?, review_count = ?, user_ratings_total = ?,
                price_level = ?, source_flags = ?, keyword_vector = ?, updated_at = ?
            WHERE naver_place_id = ?
        """;

        Timestamp now = Timestamp.from(Instant.now());

        jdbcTemplate.update(sql,
            place.getName(),
            place.getDescription(),
            place.getLatitude(),
            place.getLongitude(),
            place.getCategory(),
            place.getRating(),
            place.getAddress(),
            place.getRoadAddress(),
            place.getPhone(),
            place.getGooglePlaceId(),
            place.getOpeningHours() != null ? place.getOpeningHours().toString() : "{}",
            place.getTags() != null ? place.getTags().toArray(new String[0]) : new String[0],
            place.getAmenities() != null ? place.getAmenities().toArray(new String[0]) : new String[0],
            place.getImages() != null ? place.getImages().toArray(new String[0]) : new String[0],
            place.getGallery() != null ? place.getGallery().toArray(new String[0]) : new String[0],
            place.getReviewCount(),
            place.getUserRatingsTotal(),
            place.getPriceLevel(),
            place.getSourceFlags() != null ? place.getSourceFlags().toString() : "{}",
            place.getKeywordVector() != null ? place.getKeywordVector().toString() : null,
            now,
            place.getNaverPlaceId()
        );

        logger.debug("  🔄 Updated place: {} (ID: {})", place.getName(), place.getNaverPlaceId());
    }

    /**
     * Get statistics about written places
     */
    public WriterStats getStats() {
        return new WriterStats(totalWritten.get(), chunkCount.get());
    }

    /**
     * Reset writer statistics
     */
    public void reset() {
        totalWritten.set(0);
        chunkCount.set(0);
        logger.info("🔄 DatabasePlaceWriter stats reset");
    }

    public static class WriterStats {
        private final int totalWritten;
        private final int chunksProcessed;

        public WriterStats(int totalWritten, int chunksProcessed) {
            this.totalWritten = totalWritten;
            this.chunksProcessed = chunksProcessed;
        }

        public int getTotalWritten() { return totalWritten; }
        public int getChunksProcessed() { return chunksProcessed; }
        public double getAvgChunkSize() {
            return chunksProcessed > 0 ? (double) totalWritten / chunksProcessed : 0;
        }

        @Override
        public String toString() {
            return String.format("WriterStats{written=%d, chunks=%d, avgChunkSize=%.1f}",
                               totalWritten, chunksProcessed, getAvgChunkSize());
        }
    }
}