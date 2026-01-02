package com.mohe.batch.controller;

import com.mohe.batch.dto.ApiResponse;
import com.mohe.batch.dto.queue.WorkerInfo;
import com.mohe.batch.service.BatchStatusService;
import com.mohe.batch.service.queue.QueueMonitorService;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.sql.DataSource;
import java.sql.Connection;
import java.time.LocalDateTime;
import java.util.*;

@RestController
public class HealthController {

    private final DataSource dataSource;
    private final BatchStatusService batchStatusService;
    private final QueueMonitorService queueMonitorService;
    private final JobExplorer jobExplorer;
    private final int totalWorkers;
    private final LocalDateTime startupTime;

    public HealthController(
            DataSource dataSource,
            BatchStatusService batchStatusService,
            QueueMonitorService queueMonitorService,
            JobExplorer jobExplorer,
            @Value("${batch.worker.total-workers:3}") int totalWorkers
    ) {
        this.dataSource = dataSource;
        this.batchStatusService = batchStatusService;
        this.queueMonitorService = queueMonitorService;
        this.jobExplorer = jobExplorer;
        this.totalWorkers = totalWorkers;
        this.startupTime = LocalDateTime.now();
    }

    @GetMapping("/health")
    public ResponseEntity<ApiResponse<Map<String, Object>>> health() {
        Map<String, Object> health = new HashMap<>();
        health.put("status", "UP");
        health.put("timestamp", LocalDateTime.now());
        health.put("startupTime", startupTime);
        health.put("service", "mohe-batch");

        // Database health
        try (Connection connection = dataSource.getConnection()) {
            health.put("database", "UP");
        } catch (Exception e) {
            health.put("database", "DOWN");
            health.put("databaseError", e.getMessage());
        }

        // Worker status summary
        int runningWorkers = 0;
        for (int i = 0; i < totalWorkers; i++) {
            if (batchStatusService.isWorkerRunning(i)) {
                runningWorkers++;
            }
        }

        Map<String, Object> workers = new HashMap<>();
        workers.put("total", totalWorkers);
        workers.put("running", runningWorkers);
        health.put("workers", workers);

        return ResponseEntity.ok(ApiResponse.success(health));
    }

    @GetMapping("/")
    public ResponseEntity<ApiResponse<Map<String, Object>>> root() {
        Map<String, Object> info = new HashMap<>();
        info.put("service", "Mohe Batch Server");
        info.put("version", "1.0.0");
        info.put("status", "Running");
        info.put("endpoints", Map.of(
                "health", "/health",
                "currentJobs", "/batch/current-jobs",
                "batchStatus", "/batch/status",
                "startWorker", "POST /batch/start/{workerId}",
                "startAll", "POST /batch/start-all",
                "stopWorker", "POST /batch/stop/{workerId}",
                "stopAll", "POST /batch/stop-all",
                "embeddingStart", "POST /batch/embedding/start",
                "embeddingStatus", "/batch/embedding/status"
        ));

        return ResponseEntity.ok(ApiResponse.success(info));
    }

    /**
     * 현재 실행 중인 모든 작업 상태 조회 (종합)
     * - Queue Workers: 업데이트 작업
     * - Batch Workers: 크롤링 작업
     * - Embedding Jobs: 임베딩 작업
     */
    @GetMapping("/batch/current-jobs")
    public ResponseEntity<ApiResponse<Map<String, Object>>> getCurrentJobs() {
        Map<String, Object> result = new HashMap<>();
        result.put("timestamp", LocalDateTime.now());
        result.put("hostname", queueMonitorService.getCurrentHostname());

        List<Map<String, Object>> activeJobs = new ArrayList<>();

        // 1. Queue Workers (업데이트 작업)
        Map<String, WorkerInfo> queueWorkers = queueMonitorService.getLocalWorkers();
        for (WorkerInfo worker : queueWorkers.values()) {
            if ("active".equals(worker.getStatus()) && worker.getCurrentTaskId() != null) {
                Map<String, Object> job = new HashMap<>();
                job.put("type", "UPDATE");
                job.put("description", "장소 업데이트 (메뉴/이미지/리뷰)");
                job.put("workerId", worker.getWorkerId());
                job.put("taskId", worker.getCurrentTaskId());
                job.put("status", "PROCESSING");
                job.put("processedCount", worker.getTasksProcessed());
                job.put("failedCount", worker.getTasksFailed());
                activeJobs.add(job);
            }
        }

        // 2. Batch Workers (크롤링 작업)
        for (int i = 0; i < totalWorkers; i++) {
            BatchStatusService.WorkerStatus workerStatus = batchStatusService.getWorkerStatus(i);
            if (workerStatus != null && workerStatus.getJobExecutionId() != null) {
                JobExecution execution = batchStatusService.getJobExecution(workerStatus.getJobExecutionId());
                if (execution != null && (execution.getStatus() == BatchStatus.STARTED ||
                        execution.getStatus() == BatchStatus.STARTING)) {
                    Map<String, Object> job = new HashMap<>();
                    job.put("type", "CRAWLING");
                    job.put("description", "신규 장소 크롤링");
                    job.put("workerId", i);
                    job.put("jobExecutionId", workerStatus.getJobExecutionId());
                    job.put("status", execution.getStatus().toString());

                    // Step execution details
                    for (StepExecution step : execution.getStepExecutions()) {
                        job.put("readCount", step.getReadCount());
                        job.put("writeCount", step.getWriteCount());
                        job.put("skipCount", step.getSkipCount());
                    }

                    activeJobs.add(job);
                }
            }
        }

        // 3. Embedding Jobs (임베딩 작업)
        Set<JobExecution> embeddingJobs = jobExplorer.findRunningJobExecutions("embeddingJob");
        for (JobExecution execution : embeddingJobs) {
            Map<String, Object> job = new HashMap<>();
            job.put("type", "EMBEDDING_KEYWORD");
            job.put("description", "키워드 임베딩 생성");
            job.put("jobExecutionId", execution.getId());
            job.put("status", execution.getStatus().toString());

            for (StepExecution step : execution.getStepExecutions()) {
                job.put("readCount", step.getReadCount());
                job.put("writeCount", step.getWriteCount());
            }
            activeJobs.add(job);
        }

        Set<JobExecution> menuEmbeddingJobs = jobExplorer.findRunningJobExecutions("menuEmbeddingJob");
        for (JobExecution execution : menuEmbeddingJobs) {
            Map<String, Object> job = new HashMap<>();
            job.put("type", "EMBEDDING_MENU");
            job.put("description", "메뉴 임베딩 생성");
            job.put("jobExecutionId", execution.getId());
            job.put("status", execution.getStatus().toString());

            for (StepExecution step : execution.getStepExecutions()) {
                job.put("readCount", step.getReadCount());
                job.put("writeCount", step.getWriteCount());
            }
            activeJobs.add(job);
        }

        Set<JobExecution> allEmbeddingJobs = jobExplorer.findRunningJobExecutions("allEmbeddingJob");
        for (JobExecution execution : allEmbeddingJobs) {
            Map<String, Object> job = new HashMap<>();
            job.put("type", "EMBEDDING_ALL");
            job.put("description", "전체 임베딩 생성 (키워드+메뉴)");
            job.put("jobExecutionId", execution.getId());
            job.put("status", execution.getStatus().toString());

            for (StepExecution step : execution.getStepExecutions()) {
                job.put("readCount", step.getReadCount());
                job.put("writeCount", step.getWriteCount());
            }
            activeJobs.add(job);
        }

        result.put("activeJobCount", activeJobs.size());
        result.put("activeJobs", activeJobs);

        // Summary
        Map<String, Object> summary = new HashMap<>();
        summary.put("updateWorkers", queueWorkers.size());
        summary.put("crawlingWorkers", (int) activeJobs.stream()
                .filter(j -> "CRAWLING".equals(j.get("type"))).count());
        summary.put("embeddingJobs", (int) activeJobs.stream()
                .filter(j -> j.get("type").toString().startsWith("EMBEDDING")).count());
        result.put("summary", summary);

        return ResponseEntity.ok(ApiResponse.success(result));
    }
}
