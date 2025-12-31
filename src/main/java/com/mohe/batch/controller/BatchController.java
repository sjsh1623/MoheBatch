package com.mohe.batch.controller;

import com.mohe.batch.dto.ApiResponse;
import com.mohe.batch.service.BatchStatusService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.*;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.JobOperator;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.LocalDateTime;
import java.util.*;

@RestController
@RequestMapping("/batch")
public class BatchController {

    private static final Logger log = LoggerFactory.getLogger(BatchController.class);

    private final JobLauncher jobLauncher;
    private final JobOperator jobOperator;
    private final Job crawlingJob;
    private final BatchStatusService batchStatusService;
    private final int totalWorkers;
    private final int threadsPerWorker;
    private final int chunkSize;

    public BatchController(
            JobLauncher jobLauncher,
            JobOperator jobOperator,
            Job crawlingJob,
            BatchStatusService batchStatusService,
            @Value("${batch.worker.total-workers:10}") int totalWorkers,
            @Value("${batch.worker.threads-per-worker:1}") int threadsPerWorker,
            @Value("${batch.chunk-size:10}") int chunkSize
    ) {
        this.jobLauncher = jobLauncher;
        this.jobOperator = jobOperator;
        this.crawlingJob = crawlingJob;
        this.batchStatusService = batchStatusService;
        this.totalWorkers = totalWorkers;
        this.threadsPerWorker = threadsPerWorker;
        this.chunkSize = chunkSize;
    }

    /**
     * 특정 워커로 배치 작업 시작
     */
    @PostMapping("/start/{workerId}")
    public ResponseEntity<ApiResponse<Map<String, Object>>> startBatch(@PathVariable int workerId) {
        log.info("Received request to start batch for worker: {}", workerId);

        if (workerId < 0 || workerId >= totalWorkers) {
            return ResponseEntity.badRequest().body(
                    ApiResponse.error("INVALID_WORKER_ID",
                            String.format("Worker ID must be between 0 and %d", totalWorkers - 1))
            );
        }

        if (batchStatusService.isWorkerRunning(workerId)) {
            return ResponseEntity.badRequest().body(
                    ApiResponse.error("WORKER_ALREADY_RUNNING",
                            String.format("Worker %d is already running", workerId))
            );
        }

        try {
            JobParameters jobParameters = new JobParametersBuilder()
                    .addLong("workerId", (long) workerId)
                    .addLong("totalWorkers", (long) totalWorkers)
                    .addString("startTime", LocalDateTime.now().toString())
                    .toJobParameters();

            JobExecution execution = jobLauncher.run(crawlingJob, jobParameters);

            batchStatusService.updateWorkerStatus(workerId, execution.getStatus(), execution.getId());

            Map<String, Object> result = new HashMap<>();
            result.put("workerId", workerId);
            result.put("jobExecutionId", execution.getId());
            result.put("status", execution.getStatus().toString());
            result.put("startTime", execution.getStartTime());

            log.info("Batch job started for worker {}: executionId={}", workerId, execution.getId());
            return ResponseEntity.ok(ApiResponse.success(result));

        } catch (JobExecutionAlreadyRunningException e) {
            log.warn("Job already running for worker {}", workerId);
            return ResponseEntity.badRequest().body(
                    ApiResponse.error("JOB_ALREADY_RUNNING", "Job is already running for this worker")
            );
        } catch (Exception e) {
            log.error("Failed to start batch for worker {}: {}", workerId, e.getMessage(), e);
            return ResponseEntity.internalServerError().body(
                    ApiResponse.error("JOB_START_FAILED", e.getMessage())
            );
        }
    }

    /**
     * 모든 워커 동시에 시작
     */
    @PostMapping("/start-all")
    public ResponseEntity<ApiResponse<Map<String, Object>>> startAllWorkers() {
        log.info("Received request to start all {} workers", totalWorkers);

        Map<String, Object> results = new HashMap<>();
        List<Map<String, Object>> started = new ArrayList<>();
        List<Map<String, Object>> failed = new ArrayList<>();

        for (int workerId = 0; workerId < totalWorkers; workerId++) {
            if (batchStatusService.isWorkerRunning(workerId)) {
                Map<String, Object> failInfo = new HashMap<>();
                failInfo.put("workerId", workerId);
                failInfo.put("reason", "Already running");
                failed.add(failInfo);
                continue;
            }

            try {
                JobParameters jobParameters = new JobParametersBuilder()
                        .addLong("workerId", (long) workerId)
                        .addLong("totalWorkers", (long) totalWorkers)
                        .addString("startTime", LocalDateTime.now().toString())
                        .toJobParameters();

                JobExecution execution = jobLauncher.run(crawlingJob, jobParameters);
                batchStatusService.updateWorkerStatus(workerId, execution.getStatus(), execution.getId());

                Map<String, Object> successInfo = new HashMap<>();
                successInfo.put("workerId", workerId);
                successInfo.put("jobExecutionId", execution.getId());
                successInfo.put("status", execution.getStatus().toString());
                started.add(successInfo);

            } catch (Exception e) {
                Map<String, Object> failInfo = new HashMap<>();
                failInfo.put("workerId", workerId);
                failInfo.put("reason", e.getMessage());
                failed.add(failInfo);
            }
        }

        results.put("started", started);
        results.put("failed", failed);
        results.put("totalWorkers", totalWorkers);

        log.info("Started {}/{} workers", started.size(), totalWorkers);
        return ResponseEntity.ok(ApiResponse.success(results));
    }

    /**
     * 특정 워커 중지
     */
    @PostMapping("/stop/{workerId}")
    public ResponseEntity<ApiResponse<Map<String, Object>>> stopBatch(@PathVariable int workerId) {
        log.info("Received request to stop batch for worker: {}", workerId);

        BatchStatusService.WorkerStatus workerStatus = batchStatusService.getWorkerStatus(workerId);

        if (workerStatus == null || workerStatus.getJobExecutionId() == null) {
            return ResponseEntity.badRequest().body(
                    ApiResponse.error("WORKER_NOT_FOUND", "No running job found for worker " + workerId)
            );
        }

        try {
            boolean stopped = jobOperator.stop(workerStatus.getJobExecutionId());

            if (stopped) {
                batchStatusService.updateWorkerStatus(workerId, BatchStatus.STOPPING, workerStatus.getJobExecutionId());

                Map<String, Object> result = new HashMap<>();
                result.put("workerId", workerId);
                result.put("jobExecutionId", workerStatus.getJobExecutionId());
                result.put("status", "STOPPING");

                log.info("Stop signal sent to worker {}", workerId);
                return ResponseEntity.ok(ApiResponse.success(result));
            } else {
                return ResponseEntity.badRequest().body(
                        ApiResponse.error("STOP_FAILED", "Failed to stop worker " + workerId)
                );
            }
        } catch (Exception e) {
            log.error("Failed to stop batch for worker {}: {}", workerId, e.getMessage(), e);
            return ResponseEntity.internalServerError().body(
                    ApiResponse.error("STOP_ERROR", e.getMessage())
            );
        }
    }

    /**
     * 모든 워커 중지
     */
    @PostMapping("/stop-all")
    public ResponseEntity<ApiResponse<Map<String, Object>>> stopAllWorkers() {
        log.info("Received request to stop all workers");

        List<Map<String, Object>> stopped = new ArrayList<>();
        List<Map<String, Object>> failed = new ArrayList<>();

        for (JobExecution execution : batchStatusService.getRunningJobExecutions()) {
            try {
                jobOperator.stop(execution.getId());

                Map<String, Object> stopInfo = new HashMap<>();
                stopInfo.put("jobExecutionId", execution.getId());
                stopInfo.put("status", "STOPPING");
                stopped.add(stopInfo);
            } catch (Exception e) {
                Map<String, Object> failInfo = new HashMap<>();
                failInfo.put("jobExecutionId", execution.getId());
                failInfo.put("reason", e.getMessage());
                failed.add(failInfo);
            }
        }

        Map<String, Object> result = new HashMap<>();
        result.put("stopped", stopped);
        result.put("failed", failed);

        log.info("Sent stop signal to {} jobs", stopped.size());
        return ResponseEntity.ok(ApiResponse.success(result));
    }

    /**
     * 전체 상태 조회
     */
    @GetMapping("/status")
    public ResponseEntity<ApiResponse<Map<String, Object>>> getStatus() {
        Map<Integer, BatchStatusService.WorkerStatus> workerStatuses = batchStatusService.getAllWorkerStatuses();

        List<Map<String, Object>> workers = new ArrayList<>();
        int runningCount = 0;

        for (int i = 0; i < totalWorkers; i++) {
            BatchStatusService.WorkerStatus status = workerStatuses.get(i);
            if (status != null) {
                workers.add(status.toMap());
                if (batchStatusService.isWorkerRunning(i)) {
                    runningCount++;
                }
            } else {
                Map<String, Object> emptyStatus = new HashMap<>();
                emptyStatus.put("workerId", i);
                emptyStatus.put("status", "NOT_STARTED");
                workers.add(emptyStatus);
            }
        }

        Map<String, Object> result = new HashMap<>();
        result.put("totalWorkers", totalWorkers);
        result.put("runningCount", runningCount);
        result.put("workers", workers);

        return ResponseEntity.ok(ApiResponse.success(result));
    }

    /**
     * 특정 워커 상태 조회
     */
    @GetMapping("/status/{workerId}")
    public ResponseEntity<ApiResponse<Map<String, Object>>> getWorkerStatus(@PathVariable int workerId) {
        if (workerId < 0 || workerId >= totalWorkers) {
            return ResponseEntity.badRequest().body(
                    ApiResponse.error("INVALID_WORKER_ID",
                            String.format("Worker ID must be between 0 and %d", totalWorkers - 1))
            );
        }

        BatchStatusService.WorkerStatus status = batchStatusService.getWorkerStatus(workerId);

        if (status == null) {
            Map<String, Object> emptyStatus = new HashMap<>();
            emptyStatus.put("workerId", workerId);
            emptyStatus.put("status", "NOT_STARTED");
            return ResponseEntity.ok(ApiResponse.success(emptyStatus));
        }

        // Refresh status from job execution if available
        if (status.getJobExecutionId() != null) {
            JobExecution execution = batchStatusService.getJobExecution(status.getJobExecutionId());
            if (execution != null) {
                status.setStatus(execution.getStatus());
            }
        }

        return ResponseEntity.ok(ApiResponse.success(status.toMap()));
    }

    /**
     * 서버 설정 조회 - 워커 수, 스레드 수, 청크 크기 등
     */
    @GetMapping("/config")
    public ResponseEntity<ApiResponse<Map<String, Object>>> getConfig() {
        Map<String, Object> config = new HashMap<>();
        config.put("totalWorkers", totalWorkers);
        config.put("maxWorkers", 10); // UI can support up to 10 workers
        config.put("threadsPerWorker", threadsPerWorker);
        config.put("chunkSize", chunkSize);

        return ResponseEntity.ok(ApiResponse.success(config));
    }
}
