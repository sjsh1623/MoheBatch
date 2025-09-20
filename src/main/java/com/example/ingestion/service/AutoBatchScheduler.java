package com.example.ingestion.service;

import com.example.ingestion.service.CheckpointService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/**
 * Automatic batch scheduler that runs place ingestion jobs on startup and periodically
 */
@Service
public class AutoBatchScheduler {

    private static final Logger logger = LoggerFactory.getLogger(AutoBatchScheduler.class);

    @Autowired
    private JobLauncher jobLauncher;

    @Autowired
    @Qualifier("optimizedPlaceIngestionJob")
    private Job placeIngestionJob;

    @Autowired
    private CheckpointService checkpointService;

    @Value("${app.batch.auto-start:true}")
    private boolean autoStart;

    @Value("${app.batch.scheduling.enabled:true}")
    private boolean schedulingEnabled;

    @Value("${app.checkpoint.enabled:true}")
    private boolean checkpointEnabled;

    @Value("${app.checkpoint.auto-resume:true}")
    private boolean autoResume;

    private int executionCount = 0;

    /**
     * Run batch job automatically when application starts
     */
    @EventListener(ApplicationReadyEvent.class)
    public void runOnStartup() {
        if (autoStart) {
            logger.info("🚀 Auto-starting batch job on application startup...");
            try {
                Thread.sleep(5000); // Wait 5 seconds for full initialization

                // 체크포인트 시스템이 활성화된 경우 진행 상태 확인
                if (checkpointEnabled) {
                    boolean hasInterrupted = checkpointService.hasInterruptedBatch("place-ingestion-batch");
                    if (hasInterrupted && autoResume) {
                        logger.info("🔄 중단된 배치 발견 - 자동 재시작합니다");
                    }

                    CheckpointService.BatchProgress progress =
                        checkpointService.getBatchProgress("place-ingestion-batch");
                    if (progress.getTotal() > 0) {
                        logger.info("📊 현재 진행 상태: {}", progress);
                    }
                }

                runBatchJob("startup");
            } catch (Exception e) {
                logger.error("❌ Failed to run startup batch job", e);
            }
        } else {
            logger.info("⏸️ Auto-start disabled, batch job will only run on schedule");
        }
    }

    /**
     * Scheduled batch execution - runs every 5 minutes
     */
    @Scheduled(cron = "${app.batch.scheduling.cron:0 */1 * * * ?}")
    public void runScheduledBatch() {
        if (schedulingEnabled) {
            logger.info("⏰ Running scheduled batch job...");
            try {
                runBatchJob("scheduled");
            } catch (Exception e) {
                logger.error("❌ Failed to run scheduled batch job", e);
            }
        } else {
            logger.debug("📅 Scheduled execution disabled");
        }
    }

    /**
     * Run the batch job with unique parameters
     */
    private void runBatchJob(String trigger) {
        try {
            executionCount++;
            String timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));

            JobParameters jobParameters = new JobParametersBuilder()
                    .addString("trigger", trigger)
                    .addString("timestamp", timestamp)
                    .addLong("execution", (long) executionCount)
                    .addLong("time", System.currentTimeMillis()) // Ensure uniqueness
                    .toJobParameters();

            logger.info("🎯 Starting batch job #{} (trigger: {}) with timestamp: {}",
                       executionCount, trigger, timestamp);

            var jobExecution = jobLauncher.run(placeIngestionJob, jobParameters);

            logger.info("✅ Batch job #{} completed with status: {} (execution ID: {})",
                       executionCount, jobExecution.getStatus(), jobExecution.getId());

        } catch (Exception e) {
            logger.error("❌ Batch job #{} failed (trigger: {})", executionCount, trigger, e);
        }
    }

    /**
     * Manual trigger for batch job
     */
    public void triggerManualBatch() {
        logger.info("👆 Manually triggered batch job");
        runBatchJob("manual");
    }

    /**
     * Get execution statistics
     */
    public String getStats() {
        StringBuilder stats = new StringBuilder();
        stats.append(String.format("AutoBatchScheduler{executions=%d, autoStart=%s, scheduling=%s",
                                  executionCount, autoStart, schedulingEnabled));

        if (checkpointEnabled) {
            CheckpointService.BatchProgress progress =
                checkpointService.getBatchProgress("place-ingestion-batch");
            stats.append(String.format(", checkpoint=%s}", progress));
        } else {
            stats.append(", checkpoint=disabled}");
        }

        return stats.toString();
    }

    /**
     * 체크포인트 진행 상태 조회
     */
    public CheckpointService.BatchProgress getCheckpointProgress() {
        if (checkpointEnabled) {
            return checkpointService.getBatchProgress("place-ingestion-batch");
        }
        return new CheckpointService.BatchProgress(0, 0, 0, 0);
    }
}