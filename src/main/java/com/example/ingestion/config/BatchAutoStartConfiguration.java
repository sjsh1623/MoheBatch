package com.example.ingestion.config;

import com.example.ingestion.service.ContinuousBatchService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * 배치 자동 시작 설정
 * 요구사항: "API Token 만료 전까지 무한 반복, 사용자 개입 없이 지속 실행"
 */
@Configuration
@ConditionalOnProperty(
        value = "app.batch.continuous.enabled",
        havingValue = "true",
        matchIfMissing = false
)
public class BatchAutoStartConfiguration {

    private static final Logger logger = LoggerFactory.getLogger(BatchAutoStartConfiguration.class);

    @Value("${BATCH_AUTO_START:false}")
    private boolean batchAutoStart;

    /**
     * 애플리케이션 시작 시 무한 반복 배치를 자동으로 시작합니다
     */
    @Bean
    @ConditionalOnProperty(value = "BATCH_AUTO_START", havingValue = "true")
    public ApplicationRunner batchAutoStarter(ContinuousBatchService continuousBatchService) {
        return args -> {
            logger.info("🚀 Auto-starting continuous batch processing as configured");
            logger.info("📋 Configuration:");
            logger.info("   - BATCH_AUTO_START: {}", batchAutoStart);
            logger.info("   - Infinite loop processing: ENABLED");
            logger.info("   - Auto database initialization: ENABLED");
            logger.info("   - Target: 대한민국 전국 시/군/구/동 단위 장소 수집");

            try {
                // Start the continuous batch processing
                continuousBatchService.startContinuousProcessing();
                logger.info("✅ Continuous batch processing started successfully");
                logger.info("🔄 Batch will continue running until API tokens expire or manual stop");

            } catch (Exception e) {
                logger.error("❌ Failed to start continuous batch processing", e);
                throw new RuntimeException("Batch auto-start failed", e);
            }
        };
    }

    /**
     * 컨테이너 종료 시 배치 정리
     */
    @Bean
    @ConditionalOnProperty(value = "BATCH_AUTO_START", havingValue = "true")
    public ApplicationRunner batchShutdownHook(ContinuousBatchService continuousBatchService) {
        return args -> {
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                logger.info("🛑 Application shutdown detected, stopping continuous batch processing");
                try {
                    continuousBatchService.stopContinuousProcessing();
                    logger.info("✅ Continuous batch processing stopped gracefully");
                } catch (Exception e) {
                    logger.warn("⚠️ Error during batch shutdown: {}", e.getMessage());
                }
            }));
        };
    }
}