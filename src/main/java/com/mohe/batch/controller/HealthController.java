package com.mohe.batch.controller;

import com.mohe.batch.dto.ApiResponse;
import com.mohe.batch.service.BatchStatusService;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.sql.DataSource;
import java.sql.Connection;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;

@RestController
public class HealthController {

    private final DataSource dataSource;
    private final BatchStatusService batchStatusService;
    private final int totalWorkers;
    private final LocalDateTime startupTime;

    public HealthController(
            DataSource dataSource,
            BatchStatusService batchStatusService,
            @Value("${batch.worker.total-workers:3}") int totalWorkers
    ) {
        this.dataSource = dataSource;
        this.batchStatusService = batchStatusService;
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
                "batchStatus", "/api/batch/status",
                "startWorker", "POST /api/batch/start/{workerId}",
                "startAll", "POST /api/batch/start-all",
                "stopWorker", "POST /api/batch/stop/{workerId}",
                "stopAll", "POST /api/batch/stop-all"
        ));

        return ResponseEntity.ok(ApiResponse.success(info));
    }
}
