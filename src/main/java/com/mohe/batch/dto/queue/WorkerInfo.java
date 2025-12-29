package com.mohe.batch.dto.queue;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.time.LocalDateTime;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class WorkerInfo {

    private String workerId;
    private String hostname;
    private int threads;
    private boolean enabled;
    private LocalDateTime startedAt;
    private LocalDateTime lastHeartbeat;
    private long tasksProcessed;
    private long tasksFailed;
    private String status;  // starting, active, idle, stopping, stopped
    private String currentTaskId;

    public WorkerInfo(String workerId, String hostname, int threads, boolean enabled) {
        this.workerId = workerId;
        this.hostname = hostname;
        this.threads = threads;
        this.enabled = enabled;
        this.status = "starting";
        this.tasksProcessed = 0;
        this.tasksFailed = 0;
    }
}
