package com.mohe.batch.service.queue;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mohe.batch.dto.queue.QueueStats;
import com.mohe.batch.dto.queue.TaskProgress;
import com.mohe.batch.dto.queue.UpdateTask;
import com.mohe.batch.dto.queue.WorkerInfo;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.net.InetAddress;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@Service
@Slf4j
@RequiredArgsConstructor
public class QueueMonitorService {

    private static final String PENDING_QUEUE = "update:pending";
    private static final String PRIORITY_QUEUE = "update:priority";
    private static final String PROCESSING_SET = "update:processing";
    private static final String COMPLETED_SET = "update:completed";
    private static final String FAILED_SET = "update:failed";
    private static final String WORKER_REGISTRY = "workers:registry";
    private static final String PROGRESS_PREFIX = "update:progress:";

    private final StringRedisTemplate redisTemplate;
    private final ObjectMapper redisObjectMapper;

    private String cachedHostname = null;

    /**
     * 현재 서버의 hostname 조회
     */
    public String getCurrentHostname() {
        if (cachedHostname == null) {
            try {
                cachedHostname = InetAddress.getLocalHost().getHostName();
            } catch (Exception e) {
                log.warn("Failed to get hostname: {}", e.getMessage());
                cachedHostname = "unknown";
            }
        }
        return cachedHostname;
    }

    /**
     * 큐 통계 조회 (전체 워커)
     */
    public QueueStats getQueueStats() {
        return getQueueStats(false);
    }

    /**
     * 큐 통계 조회
     * @param localOnly true이면 현재 서버의 워커만 반환
     */
    public QueueStats getQueueStats(boolean localOnly) {
        QueueStats stats = new QueueStats();

        Long pendingSize = redisTemplate.opsForList().size(PENDING_QUEUE);
        Long prioritySize = redisTemplate.opsForList().size(PRIORITY_QUEUE);
        Long processingSize = redisTemplate.opsForSet().size(PROCESSING_SET);
        Long completedSize = redisTemplate.opsForSet().size(COMPLETED_SET);
        Long failedSize = redisTemplate.opsForSet().size(FAILED_SET);

        stats.setPendingCount(pendingSize != null ? pendingSize : 0);
        stats.setPriorityCount(prioritySize != null ? prioritySize : 0);
        stats.setProcessingCount(processingSize != null ? processingSize : 0);
        stats.setCompletedCount(completedSize != null ? completedSize : 0);
        stats.setFailedCount(failedSize != null ? failedSize : 0);
        stats.setLastUpdated(LocalDateTime.now());

        Map<String, WorkerInfo> workers = localOnly ? getLocalWorkers() : getActiveWorkers();
        stats.setWorkers(workers);
        stats.setTotalWorkers(workers.size());
        stats.setActiveWorkers((int) workers.values().stream()
                .filter(w -> "active".equals(w.getStatus()))
                .count());

        return stats;
    }

    /**
     * 활성 워커 조회 (전체)
     */
    public Map<String, WorkerInfo> getActiveWorkers() {
        Map<Object, Object> entries = redisTemplate.opsForHash().entries(WORKER_REGISTRY);
        Map<String, WorkerInfo> workers = new HashMap<>();

        for (Map.Entry<Object, Object> entry : entries.entrySet()) {
            try {
                WorkerInfo worker = redisObjectMapper.readValue(
                        entry.getValue().toString(),
                        WorkerInfo.class
                );
                workers.put(entry.getKey().toString(), worker);
            } catch (JsonProcessingException e) {
                log.warn("Failed to parse worker info: {}", e.getMessage());
            }
        }

        return workers;
    }

    /**
     * 현재 서버의 워커만 조회 (hostname 기반 필터링)
     */
    public Map<String, WorkerInfo> getLocalWorkers() {
        String currentHostname = getCurrentHostname();
        Map<String, WorkerInfo> allWorkers = getActiveWorkers();

        return allWorkers.entrySet().stream()
                .filter(entry -> currentHostname.equals(entry.getValue().getHostname()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    /**
     * 작업 진행 상태 조회
     */
    public TaskProgress getTaskProgress(String taskId) {
        String key = PROGRESS_PREFIX + taskId;
        Map<Object, Object> data = redisTemplate.opsForHash().entries(key);

        if (data.isEmpty()) return null;

        TaskProgress progress = new TaskProgress();
        progress.setTaskId(taskId);

        if (data.containsKey("placeId")) {
            progress.setPlaceId(Long.valueOf(data.get("placeId").toString()));
        }
        if (data.containsKey("status")) {
            progress.setStatus(data.get("status").toString());
        }
        if (data.containsKey("workerId")) {
            progress.setWorkerId(data.get("workerId").toString());
        }
        if (data.containsKey("attempts")) {
            progress.setAttempts(Integer.parseInt(data.get("attempts").toString()));
        }
        if (data.containsKey("updateMenus")) {
            progress.setUpdateMenus(Boolean.parseBoolean(data.get("updateMenus").toString()));
        }
        if (data.containsKey("updateImages")) {
            progress.setUpdateImages(Boolean.parseBoolean(data.get("updateImages").toString()));
        }
        if (data.containsKey("updateReviews")) {
            progress.setUpdateReviews(Boolean.parseBoolean(data.get("updateReviews").toString()));
        }
        if (data.containsKey("startTime")) {
            progress.setStartTime(LocalDateTime.parse(data.get("startTime").toString()));
        }
        if (data.containsKey("endTime")) {
            progress.setEndTime(LocalDateTime.parse(data.get("endTime").toString()));
        }
        if (data.containsKey("lastError")) {
            progress.setLastError(data.get("lastError").toString());
        }

        return progress;
    }

    /**
     * 실패한 Place ID 목록 조회
     */
    public Set<String> getFailedPlaceIds() {
        return redisTemplate.opsForSet().members(FAILED_SET);
    }

    /**
     * 실패한 작업 재시도
     */
    public int retryFailedTasks(boolean menus, boolean images, boolean reviews) {
        Set<String> failedIds = getFailedPlaceIds();
        if (failedIds == null || failedIds.isEmpty()) return 0;

        int count = 0;
        for (String placeIdStr : failedIds) {
            Long placeId = Long.valueOf(placeIdStr);
            UpdateTask task = UpdateTask.create(placeId, menus, images, reviews);
            try {
                String taskJson = redisObjectMapper.writeValueAsString(task);
                redisTemplate.opsForList().leftPush(PENDING_QUEUE, taskJson);
                redisTemplate.opsForSet().remove(FAILED_SET, placeIdStr);
                count++;
            } catch (JsonProcessingException e) {
                log.error("Failed to retry task for place {}: {}", placeId, e.getMessage());
            }
        }

        log.info("Re-queued {} failed tasks", count);
        return count;
    }

    /**
     * 오래된 워커 정리 (2분 이상 하트비트 없음)
     */
    @Scheduled(fixedRate = 60000)
    public void cleanupStaleWorkers() {
        Map<String, WorkerInfo> workers = getActiveWorkers();
        LocalDateTime threshold = LocalDateTime.now().minusMinutes(2);

        for (Map.Entry<String, WorkerInfo> entry : workers.entrySet()) {
            if (entry.getValue().getLastHeartbeat() != null &&
                    entry.getValue().getLastHeartbeat().isBefore(threshold)) {
                redisTemplate.opsForHash().delete(WORKER_REGISTRY, entry.getKey());
                log.warn("Removed stale worker: {} (last heartbeat: {})",
                        entry.getKey(), entry.getValue().getLastHeartbeat());
            }
        }
    }

    /**
     * 완료된 작업 정리 (선택적)
     */
    public void clearCompletedSet() {
        redisTemplate.delete(COMPLETED_SET);
        log.info("Cleared completed set");
    }

    /**
     * 실패한 작업 정리 (선택적)
     */
    public void clearFailedSet() {
        redisTemplate.delete(FAILED_SET);
        log.info("Cleared failed set");
    }
}
