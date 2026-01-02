package com.mohe.batch.controller;

import com.mohe.batch.dto.ApiResponse;
import com.mohe.batch.repository.PlaceKeywordEmbeddingRepository;
import com.mohe.batch.repository.PlaceMenuEmbeddingRepository;
import com.mohe.batch.repository.PlaceRepository;
import com.mohe.batch.service.EmbeddingClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.*;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.JobOperator;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Embedding Batch Controller
 * 임베딩 배치 작업 제어 API
 * - 키워드 임베딩: /batch/embedding/keyword/*
 * - 메뉴 임베딩: /batch/embedding/menu/*
 * - 전체 임베딩: /batch/embedding/all/*
 * - 기존 호환: /batch/embedding/start (키워드 임베딩)
 */
@RestController
@RequestMapping("/batch/embedding")
public class EmbeddingController {

    private static final Logger log = LoggerFactory.getLogger(EmbeddingController.class);

    private final JobLauncher jobLauncher;
    private final JobOperator jobOperator;
    private final JobExplorer jobExplorer;
    private final Job embeddingJob;           // 키워드 임베딩
    private final Job menuEmbeddingJob;       // 메뉴 임베딩
    private final Job allEmbeddingJob;        // 전체 임베딩
    private final PlaceRepository placeRepository;
    private final PlaceKeywordEmbeddingRepository keywordEmbeddingRepository;
    private final PlaceMenuEmbeddingRepository menuEmbeddingRepository;
    private final EmbeddingClient embeddingClient;

    private final AtomicReference<Long> currentKeywordJobExecutionId = new AtomicReference<>(null);
    private final AtomicReference<Long> currentMenuJobExecutionId = new AtomicReference<>(null);
    private final AtomicReference<Long> currentAllJobExecutionId = new AtomicReference<>(null);

    // Legacy support
    private final AtomicReference<Long> currentJobExecutionId = new AtomicReference<>(null);

    public EmbeddingController(
            JobLauncher jobLauncher,
            JobOperator jobOperator,
            JobExplorer jobExplorer,
            @Qualifier("embeddingJob") Job embeddingJob,
            @Qualifier("menuEmbeddingJob") Job menuEmbeddingJob,
            @Qualifier("allEmbeddingJob") Job allEmbeddingJob,
            PlaceRepository placeRepository,
            PlaceKeywordEmbeddingRepository keywordEmbeddingRepository,
            PlaceMenuEmbeddingRepository menuEmbeddingRepository,
            EmbeddingClient embeddingClient
    ) {
        this.jobLauncher = jobLauncher;
        this.jobOperator = jobOperator;
        this.jobExplorer = jobExplorer;
        this.embeddingJob = embeddingJob;
        this.menuEmbeddingJob = menuEmbeddingJob;
        this.allEmbeddingJob = allEmbeddingJob;
        this.placeRepository = placeRepository;
        this.keywordEmbeddingRepository = keywordEmbeddingRepository;
        this.menuEmbeddingRepository = menuEmbeddingRepository;
        this.embeddingClient = embeddingClient;
    }

    // Legacy field for backward compatibility
    private PlaceKeywordEmbeddingRepository embeddingRepository() {
        return keywordEmbeddingRepository;
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
     * 임베딩 배치 작업 상태 조회 (키워드 - 기존 호환)
     */
    @GetMapping("/status")
    public ResponseEntity<ApiResponse<Map<String, Object>>> getStatus() {
        return getKeywordStatus();
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

    // ==================== 키워드 임베딩 ====================

    /**
     * 키워드 임베딩 배치 시작
     */
    @PostMapping("/keyword/start")
    public ResponseEntity<ApiResponse<Map<String, Object>>> startKeywordEmbedding() {
        log.info("Received request to start keyword embedding batch job");
        return startJob(embeddingJob, currentKeywordJobExecutionId, "keyword",
                placeRepository.countPlacesForEmbedding());
    }

    /**
     * 키워드 임베딩 배치 중지
     */
    @PostMapping("/keyword/stop")
    public ResponseEntity<ApiResponse<Map<String, Object>>> stopKeywordEmbedding() {
        return stopJob(currentKeywordJobExecutionId, "keyword");
    }

    /**
     * 키워드 임베딩 상태 조회
     */
    @GetMapping("/keyword/status")
    public ResponseEntity<ApiResponse<Map<String, Object>>> getKeywordStatus() {
        Map<String, Object> result = new HashMap<>();

        long pendingCount = placeRepository.countPlacesForEmbedding();
        long embeddedCount = placeRepository.countEmbeddedPlaces();
        long totalEmbeddings = keywordEmbeddingRepository.count();

        result.put("type", "keyword");
        result.put("pendingCount", pendingCount);
        result.put("embeddedCount", embeddedCount);
        result.put("totalEmbeddings", totalEmbeddings);

        addJobStatus(result, currentKeywordJobExecutionId.get());
        result.put("embeddingServiceUrl", embeddingClient.getServiceUrl());
        result.put("embeddingServiceAvailable", embeddingClient.isServiceAvailable());

        return ResponseEntity.ok(ApiResponse.success(result));
    }

    // ==================== 메뉴 임베딩 ====================

    /**
     * 메뉴 임베딩 배치 시작
     */
    @PostMapping("/menu/start")
    public ResponseEntity<ApiResponse<Map<String, Object>>> startMenuEmbedding() {
        log.info("Received request to start menu embedding batch job");
        return startJob(menuEmbeddingJob, currentMenuJobExecutionId, "menu",
                placeRepository.countPlacesForMenuEmbedding());
    }

    /**
     * 메뉴 임베딩 배치 중지
     */
    @PostMapping("/menu/stop")
    public ResponseEntity<ApiResponse<Map<String, Object>>> stopMenuEmbedding() {
        return stopJob(currentMenuJobExecutionId, "menu");
    }

    /**
     * 메뉴 임베딩 상태 조회
     */
    @GetMapping("/menu/status")
    public ResponseEntity<ApiResponse<Map<String, Object>>> getMenuStatus() {
        Map<String, Object> result = new HashMap<>();

        long pendingCount = placeRepository.countPlacesForMenuEmbedding();
        long embeddedCount = placeRepository.countMenuEmbeddedPlaces();
        long totalEmbeddings = menuEmbeddingRepository.count();

        result.put("type", "menu");
        result.put("pendingCount", pendingCount);
        result.put("embeddedCount", embeddedCount);
        result.put("totalEmbeddings", totalEmbeddings);

        addJobStatus(result, currentMenuJobExecutionId.get());
        result.put("embeddingServiceUrl", embeddingClient.getServiceUrl());
        result.put("embeddingServiceAvailable", embeddingClient.isServiceAvailable());

        return ResponseEntity.ok(ApiResponse.success(result));
    }

    // ==================== 전체 임베딩 (키워드 + 메뉴) ====================

    /**
     * 전체 임베딩 배치 시작 (키워드 + 메뉴)
     */
    @PostMapping("/all/start")
    public ResponseEntity<ApiResponse<Map<String, Object>>> startAllEmbedding() {
        log.info("Received request to start all embedding batch job (keyword + menu)");
        return startJob(allEmbeddingJob, currentAllJobExecutionId, "all",
                placeRepository.countPlacesForAllEmbedding());
    }

    /**
     * 전체 임베딩 배치 중지
     */
    @PostMapping("/all/stop")
    public ResponseEntity<ApiResponse<Map<String, Object>>> stopAllEmbedding() {
        return stopJob(currentAllJobExecutionId, "all");
    }

    /**
     * 전체 임베딩 상태 조회
     */
    @GetMapping("/all/status")
    public ResponseEntity<ApiResponse<Map<String, Object>>> getAllStatus() {
        Map<String, Object> result = new HashMap<>();

        // Keyword stats
        long keywordPending = placeRepository.countPlacesForEmbedding();
        long keywordEmbedded = placeRepository.countEmbeddedPlaces();
        long keywordTotal = keywordEmbeddingRepository.count();

        // Menu stats
        long menuPending = placeRepository.countPlacesForMenuEmbedding();
        long menuEmbedded = placeRepository.countMenuEmbeddedPlaces();
        long menuTotal = menuEmbeddingRepository.count();

        // All stats
        long allPending = placeRepository.countPlacesForAllEmbedding();

        result.put("type", "all");
        result.put("pendingCount", allPending);

        Map<String, Object> keywordStats = new HashMap<>();
        keywordStats.put("pendingCount", keywordPending);
        keywordStats.put("embeddedCount", keywordEmbedded);
        keywordStats.put("totalEmbeddings", keywordTotal);
        result.put("keyword", keywordStats);

        Map<String, Object> menuStats = new HashMap<>();
        menuStats.put("pendingCount", menuPending);
        menuStats.put("embeddedCount", menuEmbedded);
        menuStats.put("totalEmbeddings", menuTotal);
        result.put("menu", menuStats);

        addJobStatus(result, currentAllJobExecutionId.get());
        result.put("embeddingServiceUrl", embeddingClient.getServiceUrl());
        result.put("embeddingServiceAvailable", embeddingClient.isServiceAvailable());

        return ResponseEntity.ok(ApiResponse.success(result));
    }

    // ==================== 공통 헬퍼 메서드 ====================

    private ResponseEntity<ApiResponse<Map<String, Object>>> startJob(
            Job job,
            AtomicReference<Long> executionIdRef,
            String jobType,
            long pendingCount
    ) {
        // Check if already running
        if (isJobRunning(executionIdRef)) {
            return ResponseEntity.badRequest().body(
                    ApiResponse.error("JOB_ALREADY_RUNNING", jobType + " embedding job is already running")
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
                    .addString("type", jobType)
                    .toJobParameters();

            JobExecution execution = jobLauncher.run(job, jobParameters);
            executionIdRef.set(execution.getId());

            // Legacy support
            if ("keyword".equals(jobType)) {
                currentJobExecutionId.set(execution.getId());
            }

            Map<String, Object> result = new HashMap<>();
            result.put("type", jobType);
            result.put("jobExecutionId", execution.getId());
            result.put("status", execution.getStatus().toString());
            result.put("startTime", execution.getStartTime());
            result.put("pendingCount", pendingCount);

            log.info("{} embedding job started: executionId={}", jobType, execution.getId());
            return ResponseEntity.ok(ApiResponse.success(result));

        } catch (JobExecutionAlreadyRunningException e) {
            log.warn("{} embedding job already running", jobType);
            return ResponseEntity.badRequest().body(
                    ApiResponse.error("JOB_ALREADY_RUNNING", jobType + " embedding job is already running")
            );
        } catch (Exception e) {
            log.error("Failed to start {} embedding job: {}", jobType, e.getMessage(), e);
            return ResponseEntity.internalServerError().body(
                    ApiResponse.error("JOB_START_FAILED", e.getMessage())
            );
        }
    }

    private ResponseEntity<ApiResponse<Map<String, Object>>> stopJob(
            AtomicReference<Long> executionIdRef,
            String jobType
    ) {
        log.info("Received request to stop {} embedding batch job", jobType);

        Long executionId = executionIdRef.get();
        if (executionId == null) {
            return ResponseEntity.badRequest().body(
                    ApiResponse.error("NO_RUNNING_JOB", "No " + jobType + " embedding job is currently running")
            );
        }

        try {
            boolean stopped = jobOperator.stop(executionId);

            if (stopped) {
                Map<String, Object> result = new HashMap<>();
                result.put("type", jobType);
                result.put("jobExecutionId", executionId);
                result.put("status", "STOPPING");

                log.info("Stop signal sent to {} embedding job: executionId={}", jobType, executionId);
                return ResponseEntity.ok(ApiResponse.success(result));
            } else {
                return ResponseEntity.badRequest().body(
                        ApiResponse.error("STOP_FAILED", "Failed to stop " + jobType + " embedding job")
                );
            }
        } catch (Exception e) {
            log.error("Failed to stop {} embedding job: {}", jobType, e.getMessage(), e);
            return ResponseEntity.internalServerError().body(
                    ApiResponse.error("STOP_ERROR", e.getMessage())
            );
        }
    }

    private void addJobStatus(Map<String, Object> result, Long executionId) {
        if (executionId != null) {
            JobExecution execution = jobExplorer.getJobExecution(executionId);
            if (execution != null) {
                result.put("jobExecutionId", executionId);
                result.put("jobStatus", execution.getStatus().toString());
                result.put("startTime", execution.getStartTime());
                result.put("endTime", execution.getEndTime());

                for (StepExecution stepExecution : execution.getStepExecutions()) {
                    result.put("readCount", stepExecution.getReadCount());
                    result.put("writeCount", stepExecution.getWriteCount());
                    result.put("skipCount", stepExecution.getSkipCount());
                }
            }
        } else {
            result.put("jobStatus", "NOT_STARTED");
        }
    }

    private boolean isJobRunning(AtomicReference<Long> executionIdRef) {
        Long executionId = executionIdRef.get();
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

    // Legacy support
    private boolean isJobRunning() {
        return isJobRunning(currentJobExecutionId);
    }
}
