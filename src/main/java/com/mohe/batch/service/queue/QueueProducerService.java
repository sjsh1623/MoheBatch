package com.mohe.batch.service.queue;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mohe.batch.dto.queue.UpdateTask;
import com.mohe.batch.repository.PlaceRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;

@Service
@Slf4j
@RequiredArgsConstructor
public class QueueProducerService {

    private static final String PENDING_QUEUE = "update:pending";
    private static final String PRIORITY_QUEUE = "update:priority";
    private static final String STATS_KEY = "update:stats";

    private final StringRedisTemplate redisTemplate;
    private final PlaceRepository placeRepository;
    private final ObjectMapper redisObjectMapper;

    /**
     * 단일 Place를 큐에 추가
     */
    public String pushToQueue(Long placeId, boolean menus, boolean images, boolean reviews, boolean priority) {
        UpdateTask task = UpdateTask.createWithPriority(placeId, menus, images, reviews, priority ? 1 : 0);

        String queue = priority ? PRIORITY_QUEUE : PENDING_QUEUE;
        try {
            String taskJson = redisObjectMapper.writeValueAsString(task);
            redisTemplate.opsForList().leftPush(queue, taskJson);
            redisTemplate.opsForHash().increment(STATS_KEY, "totalPushed", 1);

            log.info("Pushed task {} for place {} to {} queue", task.getTaskId(), placeId, priority ? "priority" : "pending");
            return task.getTaskId();
        } catch (JsonProcessingException e) {
            log.error("Failed to serialize task for place {}: {}", placeId, e.getMessage());
            throw new RuntimeException("Failed to push task to queue", e);
        }
    }

    /**
     * 모든 미처리 Place를 큐에 추가 (crawl_status = PENDING)
     */
    public int pushAllToQueue(boolean menus, boolean images, boolean reviews) {
        List<Long> placeIds = placeRepository.findAllPlaceIdsByCrawlStatusPending();
        log.info("Found {} unprocessed places to push to queue", placeIds.size());

        int count = 0;
        int batchSize = 100;
        List<String> batch = new ArrayList<>(batchSize);

        for (Long placeId : placeIds) {
            UpdateTask task = UpdateTask.create(placeId, menus, images, reviews);
            try {
                batch.add(redisObjectMapper.writeValueAsString(task));

                if (batch.size() >= batchSize) {
                    redisTemplate.opsForList().leftPushAll(PENDING_QUEUE, batch);
                    count += batch.size();
                    batch.clear();

                    if (count % 1000 == 0) {
                        log.info("Pushed {} places to queue...", count);
                    }
                }
            } catch (JsonProcessingException e) {
                log.error("Failed to serialize task for place {}: {}", placeId, e.getMessage());
            }
        }

        // Push remaining
        if (!batch.isEmpty()) {
            redisTemplate.opsForList().leftPushAll(PENDING_QUEUE, batch);
            count += batch.size();
        }

        redisTemplate.opsForHash().increment(STATS_KEY, "totalPushed", count);
        log.info("Pushed total {} places to queue for update", count);
        return count;
    }

    /**
     * Place ID 목록을 큐에 추가
     */
    public int pushBatchToQueue(List<Long> placeIds, boolean menus, boolean images, boolean reviews) {
        List<String> tasks = new ArrayList<>();

        for (Long placeId : placeIds) {
            UpdateTask task = UpdateTask.create(placeId, menus, images, reviews);
            try {
                tasks.add(redisObjectMapper.writeValueAsString(task));
            } catch (JsonProcessingException e) {
                log.error("Failed to serialize task for place {}: {}", placeId, e.getMessage());
            }
        }

        if (!tasks.isEmpty()) {
            redisTemplate.opsForList().leftPushAll(PENDING_QUEUE, tasks);
            redisTemplate.opsForHash().increment(STATS_KEY, "totalPushed", tasks.size());
        }

        log.info("Pushed batch of {} places to queue", tasks.size());
        return tasks.size();
    }

    /**
     * 모든 큐 초기화
     */
    public void clearQueues() {
        redisTemplate.delete(PENDING_QUEUE);
        redisTemplate.delete(PRIORITY_QUEUE);
        log.warn("All queues cleared");
    }

    /**
     * 대기 중인 작업 수 조회
     */
    public long getPendingCount() {
        Long size = redisTemplate.opsForList().size(PENDING_QUEUE);
        return size != null ? size : 0;
    }

    /**
     * 우선순위 큐 작업 수 조회
     */
    public long getPriorityCount() {
        Long size = redisTemplate.opsForList().size(PRIORITY_QUEUE);
        return size != null ? size : 0;
    }
}
