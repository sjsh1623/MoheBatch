package com.mohe.batch.controller;

import com.mohe.batch.dto.ApiResponse;
import com.mohe.batch.repository.PlaceKeywordEmbeddingRepository;
import com.mohe.batch.repository.PlaceRepository;
import com.mohe.batch.service.EmbeddingClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.*;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.JobOperator;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Embedding Batch Controller
 * 임베딩 배치 작업 제어 API
 * - 순차 처리 (병렬 없음)
 * - crawler_found = true, ready = false 조건으로 조회
 * - ORDER BY id ASC
 */
@RestController
@RequestMapping("/api/batch/embedding")
public class EmbeddingController {

    private static final Logger log = LoggerFactory.getLogger(EmbeddingController.class);

    private final JobLauncher jobLauncher;
    private final JobOperator jobOperator;
    private final JobExplorer jobExplorer;
    private final Job embeddingJob;
    private final PlaceRepository placeRepository;
    private final PlaceKeywordEmbeddingRepository embeddingRepository;
    private final EmbeddingClient embeddingClient;

    private final AtomicReference<Long> currentJobExecutionId = new AtomicReference<>(null);

    public EmbeddingController(
            JobLauncher jobLauncher,
            JobOperator jobOperator,
            JobExplorer jobExplorer,
            Job embeddingJob,
            PlaceRepository placeRepository,
            PlaceKeywordEmbeddingRepository embeddingRepository,
            EmbeddingClient embeddingClient
    ) {
        this.jobLauncher = jobLauncher;
        this.jobOperator = jobOperator;
        this.jobExplorer = jobExplorer;
        this.embeddingJob = embeddingJob;
        this.placeRepository = placeRepository;
        this.embeddingRepository = embeddingRepository;
        this.embeddingClient = embeddingClient;
    }

    /**
     * 임베딩 배치 작업 시작
     */
    @PostMapping("/start")
    public ResponseEntity<ApiResponse<Map<String, Object>>> startEmbedding() {
        log.info("Received request to start embedding batch job");

        // Check if already running
        if (isJobRunning()) {
            return ResponseEntity.badRequest().body(
                    ApiResponse.error("JOB_ALREADY_RUNNING", "Embedding job is already running")
            );
        }

        // Check embedding service availability
        if (!embeddingClient.isServiceAvailable()) {
            log.warn("Embedding service is not available at: {}", embeddingClient.getServiceUrl());
            return ResponseEntity.badRequest().body(
                    ApiResponse.error("EMBEDDING_SERVICE_UNAVAILABLE",
                            "Embedding service is not available at: " + embeddingClient.getServiceUrl())
            );
        }

        try {
            JobParameters jobParameters = new JobParametersBuilder()
                    .addString("startTime", LocalDateTime.now().toString())
                    .toJobParameters();

            JobExecution execution = jobLauncher.run(embeddingJob, jobParameters);
            currentJobExecutionId.set(execution.getId());

            Map<String, Object> result = new HashMap<>();
            result.put("jobExecutionId", execution.getId());
            result.put("status", execution.getStatus().toString());
            result.put("startTime", execution.getStartTime());
            result.put("pendingCount", placeRepository.countPlacesForEmbedding());

            log.info("Embedding job started: executionId={}", execution.getId());
            return ResponseEntity.ok(ApiResponse.success(result));

        } catch (JobExecutionAlreadyRunningException e) {
            log.warn("Embedding job already running");
            return ResponseEntity.badRequest().body(
                    ApiResponse.error("JOB_ALREADY_RUNNING", "Embedding job is already running")
            );
        } catch (Exception e) {
            log.error("Failed to start embedding job: {}", e.getMessage(), e);
            return ResponseEntity.internalServerError().body(
                    ApiResponse.error("JOB_START_FAILED", e.getMessage())
            );
        }
    }

    /**
     * 임베딩 배치 작업 중지
     */
    @PostMapping("/stop")
    public ResponseEntity<ApiResponse<Map<String, Object>>> stopEmbedding() {
        log.info("Received request to stop embedding batch job");

        Long executionId = currentJobExecutionId.get();
        if (executionId == null) {
            return ResponseEntity.badRequest().body(
                    ApiResponse.error("NO_RUNNING_JOB", "No embedding job is currently running")
            );
        }

        try {
            boolean stopped = jobOperator.stop(executionId);

            if (stopped) {
                Map<String, Object> result = new HashMap<>();
                result.put("jobExecutionId", executionId);
                result.put("status", "STOPPING");

                log.info("Stop signal sent to embedding job: executionId={}", executionId);
                return ResponseEntity.ok(ApiResponse.success(result));
            } else {
                return ResponseEntity.badRequest().body(
                        ApiResponse.error("STOP_FAILED", "Failed to stop embedding job")
                );
            }
        } catch (Exception e) {
            log.error("Failed to stop embedding job: {}", e.getMessage(), e);
            return ResponseEntity.internalServerError().body(
                    ApiResponse.error("STOP_ERROR", e.getMessage())
            );
        }
    }

    /**
     * 임베딩 배치 작업 상태 조회
     */
    @GetMapping("/status")
    public ResponseEntity<ApiResponse<Map<String, Object>>> getStatus() {
        Map<String, Object> result = new HashMap<>();

        // Get counts
        long pendingCount = placeRepository.countPlacesForEmbedding();
        long embeddedCount = placeRepository.countEmbeddedPlaces();
        long totalEmbeddings = embeddingRepository.count();

        result.put("pendingCount", pendingCount);
        result.put("embeddedCount", embeddedCount);
        result.put("totalEmbeddings", totalEmbeddings);

        // Get current job status
        Long executionId = currentJobExecutionId.get();
        if (executionId != null) {
            JobExecution execution = jobExplorer.getJobExecution(executionId);
            if (execution != null) {
                result.put("jobExecutionId", executionId);
                result.put("jobStatus", execution.getStatus().toString());
                result.put("startTime", execution.getStartTime());
                result.put("endTime", execution.getEndTime());

                // Get step execution info
                for (StepExecution stepExecution : execution.getStepExecutions()) {
                    result.put("readCount", stepExecution.getReadCount());
                    result.put("writeCount", stepExecution.getWriteCount());
                    result.put("skipCount", stepExecution.getSkipCount());
                }
            }
        } else {
            result.put("jobStatus", "NOT_STARTED");
        }

        result.put("embeddingServiceUrl", embeddingClient.getServiceUrl());
        result.put("embeddingServiceAvailable", embeddingClient.isServiceAvailable());

        return ResponseEntity.ok(ApiResponse.success(result));
    }

    /**
     * 임베딩 서비스 헬스 체크
     */
    @GetMapping("/health")
    public ResponseEntity<ApiResponse<Map<String, Object>>> checkEmbeddingHealth() {
        Map<String, Object> result = new HashMap<>();
        result.put("serviceUrl", embeddingClient.getServiceUrl());
        result.put("available", embeddingClient.isServiceAvailable());

        if (embeddingClient.isServiceAvailable()) {
            return ResponseEntity.ok(ApiResponse.success(result));
        } else {
            return ResponseEntity.ok(ApiResponse.error("EMBEDDING_SERVICE_UNAVAILABLE",
                    "Embedding service is not available"));
        }
    }

    private boolean isJobRunning() {
        Long executionId = currentJobExecutionId.get();
        if (executionId == null) {
            return false;
        }

        JobExecution execution = jobExplorer.getJobExecution(executionId);
        if (execution == null) {
            return false;
        }

        return execution.getStatus() == BatchStatus.STARTED ||
               execution.getStatus() == BatchStatus.STARTING;
    }
}
