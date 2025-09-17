package com.mohe.spring.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

@Service
public class ScheduledBatchService {

    private static final Logger logger = LoggerFactory.getLogger(ScheduledBatchService.class);

    @Autowired
    private BatchService batchService;

    private final AtomicBoolean isRunning = new AtomicBoolean(false);
    private boolean autoStartEnabled = true; // 자동 시작 활성화

    /**
     * 애플리케이션 시작 시 자동으로 배치 실행
     */
    @EventListener(ApplicationReadyEvent.class)
    public void onApplicationReady() {
        if (autoStartEnabled) {
            logger.info("Application ready - starting initial batch processing in 30 seconds...");

            // 30초 후 첫 배치 실행
            new Thread(() -> {
                try {
                    Thread.sleep(30000); // 30초 대기
                    executeInitialBatch();
                } catch (InterruptedException e) {
                    logger.error("Initial batch execution interrupted", e);
                    Thread.currentThread().interrupt();
                }
            }).start();
        }
    }

    /**
     * 초기 배치 실행
     */
    private void executeInitialBatch() {
        if (isRunning.compareAndSet(false, true)) {
            try {
                logger.info("🚀 Starting initial automated batch processing");
                Map<String, Object> result = batchService.triggerBatch();
                logger.info("✅ Initial batch processing completed: {}", result);
            } catch (Exception e) {
                logger.error("❌ Initial batch processing failed", e);
            } finally {
                isRunning.set(false);
            }
        } else {
            logger.info("Batch processing is already running, skipping initial batch");
        }
    }

    /**
     * 5분마다 자동 배치 처리 (연속적으로) - 임시 비활성화
     */
    // @Scheduled(fixedRate = 300000) // 5분 = 300,000ms
    public void automaticContinuousBatch() {
        if (isRunning.compareAndSet(false, true)) {
            try {
                logger.info("🔄 Starting scheduled continuous batch processing");

                // 추가 데이터 수집 (기존 데이터는 유지)
                int newPlaces = batchService.collectRealPlaceData();
                int newImages = batchService.generateAiImagesForPlaces();

                logger.info("✅ Scheduled batch completed: {} new places, {} new images", newPlaces, newImages);

            } catch (Exception e) {
                logger.error("❌ Scheduled batch processing failed", e);
            } finally {
                isRunning.set(false);
            }
        } else {
            logger.info("Previous batch is still running, skipping this cycle");
        }
    }

    /**
     * 매 30분마다 상태 확인 및 보고
     */
    @Scheduled(fixedRate = 1800000) // 30분 = 1,800,000ms
    public void statusReport() {
        try {
            Map<String, Object> status = batchService.getBatchStatus();
            logger.info("📊 System Status Report: {}", status);
        } catch (Exception e) {
            logger.error("Failed to generate status report", e);
        }
    }

    /**
     * 배치 처리가 현재 실행 중인지 확인
     */
    public boolean isBatchRunning() {
        return isRunning.get();
    }

    /**
     * 자동 시작 활성화/비활성화
     */
    public void setAutoStartEnabled(boolean enabled) {
        this.autoStartEnabled = enabled;
        logger.info("Auto-start batch processing: {}", enabled ? "ENABLED" : "DISABLED");
    }

    /**
     * 즉시 배치 실행 (수동 트리거)
     */
    public Map<String, Object> triggerImmediateBatch() {
        if (isRunning.compareAndSet(false, true)) {
            try {
                logger.info("🚀 Manual batch processing triggered");
                Map<String, Object> result = batchService.triggerBatch();
                logger.info("✅ Manual batch processing completed: {}", result);
                return result;
            } catch (Exception e) {
                logger.error("❌ Manual batch processing failed", e);
                return Map.of("status", "error", "message", e.getMessage());
            } finally {
                isRunning.set(false);
            }
        } else {
            return Map.of("status", "error", "message", "Batch processing is already running");
        }
    }
}