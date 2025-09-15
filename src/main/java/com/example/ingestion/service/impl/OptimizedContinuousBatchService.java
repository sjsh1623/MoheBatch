package com.example.ingestion.service.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.*;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Optimized continuous batch service for infinite processing
 * Implements requirement: "API 토큰이 만료될 때까지 무한 반복적으로 실행되는 구조"
 *
 * Performance improvements:
 * - Intelligent retry with exponential backoff
 * - Memory management between batches
 * - Error recovery without stopping execution
 * - Comprehensive monitoring and logging
 */
@Service
@ConditionalOnProperty(
    value = "app.batch.continuous.enabled",
    havingValue = "true",
    matchIfMissing = false
)
public class OptimizedContinuousBatchService implements JobExecutionListener {

    private static final Logger logger = LoggerFactory.getLogger(OptimizedContinuousBatchService.class);

    private final JobLauncher jobLauncher;
    private final Job optimizedPlaceIngestionJob;

    // State management
    private final AtomicBoolean isRunning = new AtomicBoolean(false);
    private final AtomicLong jobCounter = new AtomicLong(0);
    private final AtomicLong successCount = new AtomicLong(0);
    private final AtomicLong failureCount = new AtomicLong(0);
    private final AtomicLong totalProcessingTime = new AtomicLong(0);

    // Configuration
    private static final long SUCCESS_DELAY_MS = 2000; // 2 seconds between successful batches
    private static final long FAILURE_DELAY_MS = 60000; // 60 seconds after failure
    private static final long MAX_BACKOFF_MS = 300000; // 5 minutes max backoff
    private static final int MEMORY_CLEANUP_FREQUENCY = 10; // Every 10 batches

    private LocalDateTime startTime;
    private volatile long currentBackoffMs = FAILURE_DELAY_MS;

    public OptimizedContinuousBatchService(
            JobLauncher jobLauncher,
            @Qualifier("optimizedPlaceIngestionJob") Job optimizedPlaceIngestionJob
    ) {
        this.jobLauncher = jobLauncher;
        this.optimizedPlaceIngestionJob = optimizedPlaceIngestionJob;
    }

    /**
     * Start continuous processing with infinite loop
     */
    public void startContinuousProcessing() {
        if (isRunning.compareAndSet(false, true)) {
            startTime = LocalDateTime.now();
            logger.info("🚀 Starting optimized continuous batch processing - INFINITE MODE");
            logger.info("📊 Configuration: success_delay={}ms, failure_delay={}ms, max_backoff={}ms",
                       SUCCESS_DELAY_MS, FAILURE_DELAY_MS, MAX_BACKOFF_MS);

            executeNextBatch();
        } else {
            logger.warn("⚠️ Continuous batch processing is already running");
        }
    }

    /**
     * Stop continuous processing
     */
    public void stopContinuousProcessing() {
        if (isRunning.compareAndSet(true, false)) {
            logger.info("🛑 Stopping optimized continuous batch processing");
            logFinalStatistics();
        }
    }

    /**
     * Execute next batch in the continuous loop
     */
    @Async("batchAsyncExecutor")
    private void executeNextBatch() {
        if (!isRunning.get()) {
            logger.info("🔴 Continuous processing stopped");
            return;
        }

        long batchNumber = jobCounter.incrementAndGet();
        long batchStartTime = System.currentTimeMillis();

        try {
            logger.info("🔄 Executing batch #{} - Total processed: {}, Success: {}, Failures: {}",
                       batchNumber, batchNumber - 1, successCount.get(), failureCount.get());

            // Create unique job parameters
            JobParameters jobParameters = new JobParametersBuilder()
                    .addLong("timestamp", System.currentTimeMillis())
                    .addString("continuous", "true")
                    .addLong("batchNumber", batchNumber)
                    .addString("mode", "optimized")
                    .toJobParameters();

            // Launch job
            JobExecution jobExecution = jobLauncher.run(optimizedPlaceIngestionJob, jobParameters);

            long batchDuration = System.currentTimeMillis() - batchStartTime;
            totalProcessingTime.addAndGet(batchDuration);

            logger.info("✅ Batch #{} started successfully in {}ms - Execution ID: {}",
                       batchNumber, batchDuration, jobExecution.getId());

            // JobExecutionListener will handle what happens after completion

        } catch (Exception e) {
            long batchDuration = System.currentTimeMillis() - batchStartTime;
            logger.error("❌ Failed to execute batch #{} after {}ms: {}",
                        batchNumber, batchDuration, e.getMessage(), e);

            failureCount.incrementAndGet();
            handleBatchFailure(batchNumber);
        }
    }

    @Override
    public void beforeJob(JobExecution jobExecution) {
        Long batchNumber = jobExecution.getJobParameters().getLong("batchNumber");
        logger.info("🎬 Starting batch #{} - Job Execution ID: {}", batchNumber, jobExecution.getId());
    }

    @Override
    public void afterJob(JobExecution jobExecution) {
        Long batchNumber = jobExecution.getJobParameters().getLong("batchNumber");
        BatchStatus status = jobExecution.getStatus();
        ExitStatus exitStatus = jobExecution.getExitStatus();

        long duration = 0;
        if (jobExecution.getStartTime() != null && jobExecution.getEndTime() != null) {
            duration = java.time.Duration.between(jobExecution.getStartTime(), jobExecution.getEndTime()).toMillis();
        }

        logger.info("🏁 Batch #{} completed - Status: {}, Exit: {}, Duration: {}ms",
                   batchNumber, status, exitStatus.getExitCode(), duration);

        switch (status) {
            case COMPLETED:
                handleBatchSuccess(batchNumber, duration);
                break;
            case FAILED:
                handleBatchFailure(batchNumber);
                break;
            case STOPPED:
                logger.warn("⏸️ Batch #{} was stopped", batchNumber);
                scheduleNextBatch(SUCCESS_DELAY_MS);
                break;
            default:
                logger.warn("❓ Batch #{} ended with unexpected status: {}", batchNumber, status);
                scheduleNextBatch(SUCCESS_DELAY_MS);
                break;
        }
    }

    /**
     * Handle successful batch completion
     */
    private void handleBatchSuccess(Long batchNumber, long duration) {
        successCount.incrementAndGet();
        currentBackoffMs = FAILURE_DELAY_MS; // Reset backoff on success

        logger.info("✅ Batch #{} completed successfully in {}ms",batchNumber, duration);

        // Memory cleanup every N batches
        if (batchNumber % MEMORY_CLEANUP_FREQUENCY == 0) {
            performMemoryCleanup(batchNumber);
        }

        // Log periodic statistics
        if (batchNumber % 5 == 0) {
            logPeriodicStatistics(batchNumber);
        }

        scheduleNextBatch(SUCCESS_DELAY_MS);
    }

    /**
     * Handle batch failure with exponential backoff
     */
    private void handleBatchFailure(Long batchNumber) {
        failureCount.incrementAndGet();

        // Exponential backoff with jitter
        currentBackoffMs = Math.min(currentBackoffMs * 2, MAX_BACKOFF_MS);
        long jitter = (long) (currentBackoffMs * 0.1 * Math.random());
        long actualDelay = currentBackoffMs + jitter;

        logger.warn("⚠️ Batch #{} failed - Waiting {}ms before retry (backoff: {}ms)",
                   batchNumber, actualDelay, currentBackoffMs);

        scheduleNextBatch(actualDelay);
    }

    /**
     * Schedule next batch execution
     */
    private void scheduleNextBatch(long delayMs) {
        if (!isRunning.get()) {
            return;
        }

        try {
            Thread.sleep(delayMs);
            executeNextBatch();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.warn("🔴 Batch scheduling interrupted, stopping continuous processing");
            stopContinuousProcessing();
        }
    }

    /**
     * Perform memory cleanup
     */
    private void performMemoryCleanup(Long batchNumber) {
        logger.info("🧹 Performing memory cleanup after batch #{}", batchNumber);

        try {
            System.gc(); // Suggest garbage collection

            Runtime runtime = Runtime.getRuntime();
            long totalMemory = runtime.totalMemory();
            long freeMemory = runtime.freeMemory();
            long usedMemory = totalMemory - freeMemory;
            long maxMemory = runtime.maxMemory();

            double memoryUsagePercent = (double) usedMemory / maxMemory * 100;

            logger.info("💾 Memory status: Used {}MB / {}MB ({:.1f}%)",
                       usedMemory / 1024 / 1024, maxMemory / 1024 / 1024, memoryUsagePercent);

            if (memoryUsagePercent > 80) {
                logger.warn("⚠️ High memory usage detected: {:.1f}%", memoryUsagePercent);
            }

        } catch (Exception e) {
            logger.warn("Failed to perform memory cleanup: {}", e.getMessage());
        }
    }

    /**
     * Log periodic statistics
     */
    private void logPeriodicStatistics(Long batchNumber) {
        long totalBatches = batchNumber;
        long uptime = Duration.between(startTime, LocalDateTime.now()).toMillis();
        double avgBatchTime = totalBatches > 0 ? (double) totalProcessingTime.get() / totalBatches : 0;
        double successRate = totalBatches > 0 ? (double) successCount.get() / totalBatches * 100 : 0;

        logger.info("📈 Statistics after {} batches:", batchNumber);
        logger.info("   ⏱️ Uptime: {}ms ({:.1f} hours)", uptime, uptime / 3600000.0);
        logger.info("   ✅ Success rate: {:.1f}% ({}/{})", successRate, successCount.get(), totalBatches);
        logger.info("   ⚡ Avg batch time: {:.1f}ms", avgBatchTime);
        logger.info("   🔄 Current backoff: {}ms", currentBackoffMs);
    }

    /**
     * Log final statistics when stopping
     */
    private void logFinalStatistics() {
        long totalBatches = jobCounter.get();
        long uptime = startTime != null ? Duration.between(startTime, LocalDateTime.now()).toMillis() : 0;
        double avgBatchTime = totalBatches > 0 ? (double) totalProcessingTime.get() / totalBatches : 0;
        double successRate = totalBatches > 0 ? (double) successCount.get() / totalBatches * 100 : 0;

        logger.info("🏁 Final Statistics:");
        logger.info("   📊 Total batches executed: {}", totalBatches);
        logger.info("   ⏱️ Total uptime: {}ms ({:.2f} hours)", uptime, uptime / 3600000.0);
        logger.info("   ✅ Successful batches: {} ({:.1f}%)", successCount.get(), successRate);
        logger.info("   ❌ Failed batches: {}", failureCount.get());
        logger.info("   ⚡ Average batch time: {:.1f}ms", avgBatchTime);
    }

    /**
     * Get current service status
     */
    public ServiceStatus getServiceStatus() {
        return new ServiceStatus(
                isRunning.get(),
                jobCounter.get(),
                successCount.get(),
                failureCount.get(),
                currentBackoffMs,
                startTime != null ? Duration.between(startTime, LocalDateTime.now()).toMillis() : 0
        );
    }

    public static class ServiceStatus {
        private final boolean running;
        private final long totalBatches;
        private final long successfulBatches;
        private final long failedBatches;
        private final long currentBackoffMs;
        private final long uptimeMs;

        public ServiceStatus(boolean running, long totalBatches, long successfulBatches,
                           long failedBatches, long currentBackoffMs, long uptimeMs) {
            this.running = running;
            this.totalBatches = totalBatches;
            this.successfulBatches = successfulBatches;
            this.failedBatches = failedBatches;
            this.currentBackoffMs = currentBackoffMs;
            this.uptimeMs = uptimeMs;
        }

        // Getters
        public boolean isRunning() { return running; }
        public long getTotalBatches() { return totalBatches; }
        public long getSuccessfulBatches() { return successfulBatches; }
        public long getFailedBatches() { return failedBatches; }
        public long getCurrentBackoffMs() { return currentBackoffMs; }
        public long getUptimeMs() { return uptimeMs; }
        public double getSuccessRate() {
            return totalBatches > 0 ? (double) successfulBatches / totalBatches * 100 : 0;
        }

        @Override
        public String toString() {
            return String.format("ServiceStatus{running=%s, total=%d, success=%d, failed=%d, " +
                               "successRate=%.1f%%, uptime=%.1fh}",
                               running, totalBatches, successfulBatches, failedBatches,
                               getSuccessRate(), uptimeMs / 3600000.0);
        }
    }
}