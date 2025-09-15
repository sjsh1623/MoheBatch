package com.example.ingestion;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * Main application class for MoheBatch
 * Java-only optimized batch processing system
 */
@SpringBootApplication
@EnableBatchProcessing
@EnableAsync
@EnableScheduling
public class DataIngestionApplication {

    private static final Logger logger = LoggerFactory.getLogger(DataIngestionApplication.class);

    public static void main(String[] args) {
        logger.info("🚀 Starting MoheBatch - Java Optimized Batch Processing System");
        logger.info("📋 Features:");
        logger.info("   ✅ OpenAI for text generation (description, keywords, image prompts)");
        logger.info("   ✅ Ollama for vector embeddings only");
        logger.info("   ✅ Comprehensive place filtering (clubs, marts, etc.)");
        logger.info("   ✅ Performance optimized with async processing");
        logger.info("   ✅ Infinite loop batch processing");
        logger.info("   ✅ Database initialization and cleanup");

        try {
            SpringApplication.run(DataIngestionApplication.class, args);
            logger.info("✅ MoheBatch application started successfully");
        } catch (Exception e) {
            logger.error("❌ Failed to start MoheBatch application", e);
            System.exit(1);
        }
    }
}