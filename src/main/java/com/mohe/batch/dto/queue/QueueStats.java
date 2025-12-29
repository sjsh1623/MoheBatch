package com.mohe.batch.dto.queue;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.time.LocalDateTime;
import java.util.Map;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class QueueStats {

    private long pendingCount;
    private long priorityCount;
    private long processingCount;
    private long completedCount;
    private long failedCount;
    private int activeWorkers;
    private int totalWorkers;
    private LocalDateTime lastUpdated;
    private Map<String, WorkerInfo> workers;
}
