package com.example.ingestion.batch.writer;

import com.example.ingestion.dto.ProcessedPlaceJava;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ItemWriter;
import org.springframework.stereotype.Component;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Mock writer for testing batch processing without database
 */
@Component
public class MockPlaceWriter implements ItemWriter<ProcessedPlaceJava> {

    private static final Logger logger = LoggerFactory.getLogger(MockPlaceWriter.class);

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

        logger.info("📝 Writing chunk #{}: {} places (total written: {})", chunkNum, chunkSize, totalSoFar);

        // Simulate processing each place
        for (ProcessedPlaceJava place : chunk.getItems()) {
            processPlace(place);
        }

        // Simulate some processing time
        Thread.sleep(100 + (int)(Math.random() * 200)); // 100-300ms delay

        logger.info("✅ Chunk #{} written successfully", chunkNum);
    }

    private void processPlace(ProcessedPlaceJava place) {
        logger.debug("  🏪 Processing: {} ({})",
                   place.getName(),
                   place.getCategory());

        // Log some details about the place
        if (place.getDescription() != null && !place.getDescription().isEmpty()) {
            logger.debug("    📝 Has AI description: {} chars", place.getDescription().length());
        }

        if (place.getKeywordVector() != null && !place.getKeywordVector().isEmpty()) {
            logger.debug("    🔢 Has embedding vector: {} dimensions", place.getKeywordVector().size());
        }

        // Log source flags
        if (place.getSourceFlags() != null) {
            boolean hasAiDesc = Boolean.TRUE.equals(place.getSourceFlags().get("hasAiDescription"));
            boolean hasEmbedding = Boolean.TRUE.equals(place.getSourceFlags().get("hasEmbedding"));
            boolean hasGoogleData = Boolean.TRUE.equals(place.getSourceFlags().get("hasGoogleData"));

            logger.debug("    🏷️ Flags: AI_DESC={}, EMBEDDING={}, GOOGLE={}", hasAiDesc, hasEmbedding, hasGoogleData);
        }
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
        logger.info("🔄 MockPlaceWriter stats reset");
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