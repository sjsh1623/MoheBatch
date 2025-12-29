package com.mohe.batch.service.queue;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mohe.batch.config.QueueWorkerConfig;
import com.mohe.batch.dto.queue.UpdateTask;
import com.mohe.batch.dto.queue.WorkerInfo;
import com.mohe.batch.entity.CrawlStatus;
import com.mohe.batch.entity.Place;
import com.mohe.batch.exception.PlaceNotFoundException;
import com.mohe.batch.repository.PlaceRepository;
import com.mohe.batch.service.UpdateProcessorService;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@Service
@Slf4j
public class QueueWorkerService {

    private static final String PENDING_QUEUE = "update:pending";
    private static final String PRIORITY_QUEUE = "update:priority";
    private static final String PROCESSING_SET = "update:processing";
    private static final String COMPLETED_SET = "update:completed";
    private static final String FAILED_SET = "update:failed";
    private static final String PROGRESS_PREFIX = "update:progress:";
    private static final String WORKER_REGISTRY = "workers:registry";
    private static final String STATS_KEY = "update:stats";
    private static final String DELETED_SET = "update:deleted";

    private static final int BRPOP_TIMEOUT_SECONDS = 30;

    private final StringRedisTemplate redisTemplate;
    private final UpdateProcessorService updateProcessorService;
    private final PlaceRepository placeRepository;
    private final ObjectMapper redisObjectMapper;
    private final WorkerInfo workerInfo;
    private final QueueWorkerConfig.QueueRetryConfig retryConfig;

    private volatile boolean running = false;
    private ExecutorService executorService;

    public QueueWorkerService(
            StringRedisTemplate redisTemplate,
            UpdateProcessorService updateProcessorService,
            PlaceRepository placeRepository,
            ObjectMapper redisObjectMapper,
            WorkerInfo workerInfo,
            QueueWorkerConfig.QueueRetryConfig retryConfig
    ) {
        this.redisTemplate = redisTemplate;
        this.updateProcessorService = updateProcessorService;
        this.placeRepository = placeRepository;
        this.redisObjectMapper = redisObjectMapper;
        this.workerInfo = workerInfo;
        this.retryConfig = retryConfig;
    }

    @PostConstruct
    public void init() {
        if (workerInfo.isEnabled()) {
            registerWorker();
            start();
        } else {
            log.info("Queue worker is disabled");
        }
    }

    @PreDestroy
    public void shutdown() {
        stop();
        unregisterWorker();
    }

    public void start() {
        if (running) {
            log.warn("Worker {} is already running", workerInfo.getWorkerId());
            return;
        }

        running = true;
        executorService = Executors.newFixedThreadPool(workerInfo.getThreads());

        for (int i = 0; i < workerInfo.getThreads(); i++) {
            final int threadNum = i;
            executorService.submit(() -> workerLoop(threadNum));
        }

        updateWorkerStatus("active");
        log.info("Worker {} started with {} threads", workerInfo.getWorkerId(), workerInfo.getThreads());
    }

    public void stop() {
        if (!running) {
            log.warn("Worker {} is not running", workerInfo.getWorkerId());
            return;
        }

        running = false;
        updateWorkerStatus("stopping");

        if (executorService != null) {
            executorService.shutdown();
            try {
                if (!executorService.awaitTermination(60, TimeUnit.SECONDS)) {
                    executorService.shutdownNow();
                }
            } catch (InterruptedException e) {
                executorService.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }

        updateWorkerStatus("stopped");
        log.info("Worker {} stopped", workerInfo.getWorkerId());
    }

    public boolean isRunning() {
        return running;
    }

    private void workerLoop(int threadNum) {
        log.info("Worker {} thread {} started", workerInfo.getWorkerId(), threadNum);

        while (running) {
            try {
                String taskJson = popFromQueues();

                if (taskJson == null) {
                    continue;
                }

                UpdateTask task = redisObjectMapper.readValue(taskJson, UpdateTask.class);
                processTask(task);

            } catch (Exception e) {
                log.error("Worker {} thread {} error: {}", workerInfo.getWorkerId(), threadNum, e.getMessage());
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }

        log.info("Worker {} thread {} stopped", workerInfo.getWorkerId(), threadNum);
    }

    private String popFromQueues() {
        // Try priority queue first (non-blocking)
        String task = redisTemplate.opsForList().rightPop(PRIORITY_QUEUE);
        if (task != null) {
            return task;
        }

        // Blocking pop from pending queue
        return redisTemplate.opsForList().rightPop(PENDING_QUEUE, Duration.ofSeconds(BRPOP_TIMEOUT_SECONDS));
    }

    private void processTask(UpdateTask task) {
        String taskId = task.getTaskId();
        Long placeId = task.getPlaceId();

        try {
            // Mark as processing
            redisTemplate.opsForSet().add(PROCESSING_SET, taskId);
            workerInfo.setCurrentTaskId(taskId);
            updateTaskProgress(task, "processing", null);

            log.info("Processing task {} for place {}", taskId, placeId);
            updateProcessorService.processUpdate(
                    placeId,
                    task.isUpdateMenus(),
                    task.isUpdateImages(),
                    task.isUpdateReviews()
            );

            // Mark as completed
            redisTemplate.opsForSet().remove(PROCESSING_SET, taskId);
            redisTemplate.opsForSet().add(COMPLETED_SET, String.valueOf(placeId));
            redisTemplate.opsForHash().increment(STATS_KEY, "totalCompleted", 1);
            updateTaskProgress(task, "completed", null);

            workerInfo.setTasksProcessed(workerInfo.getTasksProcessed() + 1);
            workerInfo.setCurrentTaskId(null);
            updateWorkerStatus("active");

            log.info("Completed task {} for place {}", taskId, placeId);

        } catch (PlaceNotFoundException e) {
            // 장소를 찾을 수 없음 (404, 폐업 등) - NOT_FOUND 상태로 변경
            handlePlaceNotFound(task, e);
        } catch (Exception e) {
            handleTaskFailure(task, e);
        }
    }

    /**
     * 장소 NOT_FOUND 처리 (PlaceNotFoundException 발생 시)
     * - crawl_status = NOT_FOUND로 변경
     * - Redis 통계 업데이트
     */
    private void handlePlaceNotFound(UpdateTask task, PlaceNotFoundException e) {
        String taskId = task.getTaskId();
        Long placeId = e.getPlaceId();
        String placeName = e.getPlaceName();

        log.warn("⚠️ Place not found, setting status to NOT_FOUND: '{}' (ID: {})", placeName, placeId);

        try {
            // DB에서 Place 조회 후 상태 업데이트
            Place place = placeRepository.findById(placeId).orElse(null);
            if (place != null) {
                place.setCrawlStatus(CrawlStatus.NOT_FOUND);
                placeRepository.saveAndFlush(place);
                log.info("⚠️ Place 상태 변경 완료: '{}' (ID: {}) -> NOT_FOUND", placeName, placeId);
            }

            // Redis 통계 업데이트
            redisTemplate.opsForSet().remove(PROCESSING_SET, taskId);
            redisTemplate.opsForSet().add(DELETED_SET, String.valueOf(placeId));
            redisTemplate.opsForHash().increment(STATS_KEY, "totalNotFound", 1);
            updateTaskProgress(task, "not_found", "Place not found - status set to NOT_FOUND");

        } catch (Exception updateError) {
            log.error("❌ Place 상태 업데이트 실패: '{}' (ID: {}) - {}", placeName, placeId, updateError.getMessage());
            // 업데이트 실패 시 failed로 처리
            redisTemplate.opsForSet().remove(PROCESSING_SET, taskId);
            redisTemplate.opsForSet().add(FAILED_SET, String.valueOf(placeId));
            redisTemplate.opsForHash().increment(STATS_KEY, "totalFailed", 1);
            updateTaskProgress(task, "failed", "Status update failed: " + updateError.getMessage());
        }

        workerInfo.setCurrentTaskId(null);
        updateWorkerStatus("active");
    }

    private void handleTaskFailure(UpdateTask task, Exception e) {
        String taskId = task.getTaskId();
        Long placeId = task.getPlaceId();

        redisTemplate.opsForSet().remove(PROCESSING_SET, taskId);
        task.setAttempts(task.getAttempts() + 1);

        if (task.getAttempts() < retryConfig.maxAttempts()) {
            // Re-queue for retry with exponential backoff
            task.setScheduledAt(LocalDateTime.now().plusSeconds(
                    (long) Math.pow(2, task.getAttempts()) * retryConfig.backoffMultiplier()
            ));

            try {
                String taskJson = redisObjectMapper.writeValueAsString(task);
                redisTemplate.opsForList().leftPush(PENDING_QUEUE, taskJson);
                redisTemplate.opsForHash().increment(STATS_KEY, "totalRetried", 1);

                log.warn("Re-queued task {} for place {} (attempt {}): {}",
                        taskId, placeId, task.getAttempts(), e.getMessage());
            } catch (JsonProcessingException je) {
                log.error("Failed to re-queue task: {}", je.getMessage());
            }

            updateTaskProgress(task, "retrying", e.getMessage());

        } else {
            // Max retries exceeded, mark as failed
            redisTemplate.opsForSet().add(FAILED_SET, String.valueOf(placeId));
            redisTemplate.opsForHash().increment(STATS_KEY, "totalFailed", 1);
            updateTaskProgress(task, "failed", e.getMessage());

            workerInfo.setTasksFailed(workerInfo.getTasksFailed() + 1);
            log.error("Task {} for place {} failed after {} attempts: {}",
                    taskId, placeId, task.getAttempts(), e.getMessage());
        }

        workerInfo.setCurrentTaskId(null);
        updateWorkerStatus("active");
    }

    private void updateTaskProgress(UpdateTask task, String status, String error) {
        String key = PROGRESS_PREFIX + task.getTaskId();
        Map<String, String> progress = new HashMap<>();
        progress.put("placeId", String.valueOf(task.getPlaceId()));
        progress.put("status", status);
        progress.put("workerId", workerInfo.getWorkerId());
        progress.put("attempts", String.valueOf(task.getAttempts()));
        progress.put("updateMenus", String.valueOf(task.isUpdateMenus()));
        progress.put("updateImages", String.valueOf(task.isUpdateImages()));
        progress.put("updateReviews", String.valueOf(task.isUpdateReviews()));

        if ("processing".equals(status)) {
            progress.put("startTime", LocalDateTime.now().toString());
        } else if ("completed".equals(status) || "failed".equals(status)) {
            progress.put("endTime", LocalDateTime.now().toString());
        }

        if (error != null) {
            progress.put("lastError", error.length() > 500 ? error.substring(0, 500) : error);
        }

        redisTemplate.opsForHash().putAll(key, progress);
        redisTemplate.expire(key, Duration.ofHours(24));
    }

    private void registerWorker() {
        workerInfo.setStartedAt(LocalDateTime.now());
        workerInfo.setLastHeartbeat(LocalDateTime.now());
        workerInfo.setStatus("starting");
        workerInfo.setTasksProcessed(0);
        workerInfo.setTasksFailed(0);

        try {
            String workerJson = redisObjectMapper.writeValueAsString(workerInfo);
            redisTemplate.opsForHash().put(WORKER_REGISTRY, workerInfo.getWorkerId(), workerJson);
            log.info("Worker {} registered on {}", workerInfo.getWorkerId(), workerInfo.getHostname());
        } catch (JsonProcessingException e) {
            log.error("Failed to register worker: {}", e.getMessage());
        }
    }

    private void unregisterWorker() {
        redisTemplate.opsForHash().delete(WORKER_REGISTRY, workerInfo.getWorkerId());
        log.info("Worker {} unregistered", workerInfo.getWorkerId());
    }

    private void updateWorkerStatus(String status) {
        workerInfo.setStatus(status);
        workerInfo.setLastHeartbeat(LocalDateTime.now());

        try {
            String workerJson = redisObjectMapper.writeValueAsString(workerInfo);
            redisTemplate.opsForHash().put(WORKER_REGISTRY, workerInfo.getWorkerId(), workerJson);
        } catch (JsonProcessingException e) {
            log.error("Failed to update worker status: {}", e.getMessage());
        }
    }

    @Scheduled(fixedRate = 10000)
    public void heartbeat() {
        if (running) {
            updateWorkerStatus("active");
        }
    }
}
