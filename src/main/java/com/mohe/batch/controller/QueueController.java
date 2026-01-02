package com.mohe.batch.controller;

import com.mohe.batch.dto.ApiResponse;
import com.mohe.batch.dto.queue.QueueStats;
import com.mohe.batch.dto.queue.TaskProgress;
import com.mohe.batch.dto.queue.WorkerInfo;
import com.mohe.batch.service.queue.QueueMonitorService;
import com.mohe.batch.service.queue.QueueProducerService;
import com.mohe.batch.service.queue.QueueWorkerService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

@RestController
@RequestMapping("/batch/queue")
@RequiredArgsConstructor
@Slf4j
@Tag(name = "Queue", description = "Redis Message Queue 기반 배치 처리 API")
public class QueueController {

    private final QueueProducerService producerService;
    private final QueueWorkerService workerService;
    private final QueueMonitorService monitorService;

    // ===== Producer API =====

    @PostMapping("/push/{placeId}")
    @Operation(summary = "단일 Place 큐에 추가")
    public ResponseEntity<ApiResponse<Map<String, Object>>> pushToQueue(
            @PathVariable Long placeId,
            @RequestParam(defaultValue = "true") boolean menus,
            @RequestParam(defaultValue = "true") boolean images,
            @RequestParam(defaultValue = "true") boolean reviews,
            @RequestParam(defaultValue = "false") boolean priority
    ) {
        String taskId = producerService.pushToQueue(placeId, menus, images, reviews, priority);

        Map<String, Object> result = new HashMap<>();
        result.put("taskId", taskId);
        result.put("placeId", placeId);
        result.put("priority", priority);

        return ResponseEntity.ok(ApiResponse.success(result));
    }

    @PostMapping("/push-all")
    @Operation(summary = "모든 미처리 Place 큐에 추가 (Update All)")
    public ResponseEntity<ApiResponse<Map<String, Object>>> pushAllToQueue(
            @RequestParam(defaultValue = "true") boolean menus,
            @RequestParam(defaultValue = "true") boolean images,
            @RequestParam(defaultValue = "true") boolean reviews
    ) {
        log.info("Push all unprocessed places to queue: menus={}, images={}, reviews={}", menus, images, reviews);
        int count = producerService.pushAllToQueue(menus, images, reviews);

        Map<String, Object> result = new HashMap<>();
        result.put("pushedCount", count);
        result.put("updateMenus", menus);
        result.put("updateImages", images);
        result.put("updateReviews", reviews);

        return ResponseEntity.ok(ApiResponse.success("Pushed " + count + " places to queue", result));
    }

    @PostMapping("/push-batch")
    @Operation(summary = "Place ID 목록 큐에 추가")
    public ResponseEntity<ApiResponse<Map<String, Object>>> pushBatchToQueue(
            @RequestBody List<Long> placeIds,
            @RequestParam(defaultValue = "true") boolean menus,
            @RequestParam(defaultValue = "true") boolean images,
            @RequestParam(defaultValue = "true") boolean reviews
    ) {
        int count = producerService.pushBatchToQueue(placeIds, menus, images, reviews);

        Map<String, Object> result = new HashMap<>();
        result.put("pushedCount", count);
        result.put("requestedCount", placeIds.size());

        return ResponseEntity.ok(ApiResponse.success(result));
    }

    // ===== Monitor API =====

    @GetMapping("/stats")
    @Operation(summary = "큐 통계 및 워커 상태 조회")
    public ResponseEntity<ApiResponse<QueueStats>> getStats(
            @RequestParam(defaultValue = "true") boolean localOnly
    ) {
        QueueStats stats = monitorService.getQueueStats(localOnly);
        return ResponseEntity.ok(ApiResponse.success(stats));
    }

    @GetMapping("/task/{taskId}")
    @Operation(summary = "작업 진행 상태 조회")
    public ResponseEntity<ApiResponse<TaskProgress>> getTaskProgress(@PathVariable String taskId) {
        TaskProgress progress = monitorService.getTaskProgress(taskId);
        if (progress == null) {
            return ResponseEntity.ok(ApiResponse.error("Task not found: " + taskId));
        }
        return ResponseEntity.ok(ApiResponse.success(progress));
    }

    @GetMapping("/workers")
    @Operation(summary = "활성 워커 목록 조회")
    public ResponseEntity<ApiResponse<Map<String, WorkerInfo>>> getWorkers(
            @RequestParam(defaultValue = "true") boolean localOnly
    ) {
        Map<String, WorkerInfo> workers = localOnly
                ? monitorService.getLocalWorkers()
                : monitorService.getActiveWorkers();
        return ResponseEntity.ok(ApiResponse.success(workers));
    }

    @GetMapping("/failed")
    @Operation(summary = "실패한 Place ID 목록 조회")
    public ResponseEntity<ApiResponse<Set<String>>> getFailedTasks() {
        Set<String> failed = monitorService.getFailedPlaceIds();
        return ResponseEntity.ok(ApiResponse.success(failed));
    }

    // ===== Worker Control API =====

    @PostMapping("/worker/start")
    @Operation(summary = "현재 인스턴스 워커 시작")
    public ResponseEntity<ApiResponse<Map<String, Object>>> startWorker() {
        workerService.start();

        Map<String, Object> result = new HashMap<>();
        result.put("status", "started");
        result.put("running", workerService.isRunning());

        return ResponseEntity.ok(ApiResponse.success("Worker started", result));
    }

    @PostMapping("/worker/stop")
    @Operation(summary = "현재 인스턴스 워커 중지")
    public ResponseEntity<ApiResponse<Map<String, Object>>> stopWorker() {
        workerService.stop();

        Map<String, Object> result = new HashMap<>();
        result.put("status", "stopped");
        result.put("running", workerService.isRunning());

        return ResponseEntity.ok(ApiResponse.success("Worker stopped", result));
    }

    @GetMapping("/worker/status")
    @Operation(summary = "현재 인스턴스 워커 상태 조회")
    public ResponseEntity<ApiResponse<Map<String, Object>>> getWorkerStatus() {
        Map<String, Object> result = new HashMap<>();
        result.put("running", workerService.isRunning());

        return ResponseEntity.ok(ApiResponse.success(result));
    }

    // ===== Recovery API =====

    @PostMapping("/retry-failed")
    @Operation(summary = "실패한 작업 모두 재시도")
    public ResponseEntity<ApiResponse<Map<String, Object>>> retryFailed(
            @RequestParam(defaultValue = "true") boolean menus,
            @RequestParam(defaultValue = "true") boolean images,
            @RequestParam(defaultValue = "true") boolean reviews
    ) {
        int count = monitorService.retryFailedTasks(menus, images, reviews);

        Map<String, Object> result = new HashMap<>();
        result.put("retriedCount", count);

        return ResponseEntity.ok(ApiResponse.success("Retried " + count + " failed tasks", result));
    }

    // ===== Admin API =====

    @DeleteMapping("/clear")
    @Operation(summary = "모든 큐 초기화 (주의)")
    public ResponseEntity<ApiResponse<String>> clearQueues() {
        producerService.clearQueues();
        return ResponseEntity.ok(ApiResponse.success("All queues cleared"));
    }

    @DeleteMapping("/clear-completed")
    @Operation(summary = "완료된 작업 Set 초기화")
    public ResponseEntity<ApiResponse<String>> clearCompleted() {
        monitorService.clearCompletedSet();
        return ResponseEntity.ok(ApiResponse.success("Completed set cleared"));
    }

    @DeleteMapping("/clear-failed")
    @Operation(summary = "실패한 작업 Set 초기화")
    public ResponseEntity<ApiResponse<String>> clearFailed() {
        monitorService.clearFailedSet();
        return ResponseEntity.ok(ApiResponse.success("Failed set cleared"));
    }
}
