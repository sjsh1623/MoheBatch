package com.mohe.batch.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class BatchStatusService {

    private static final Logger log = LoggerFactory.getLogger(BatchStatusService.class);

    private final JobExplorer jobExplorer;
    private final Map<Integer, WorkerStatus> workerStatusMap = new ConcurrentHashMap<>();

    public BatchStatusService(JobExplorer jobExplorer) {
        this.jobExplorer = jobExplorer;
    }

    public void updateWorkerStatus(int workerId, BatchStatus status, Long jobExecutionId) {
        WorkerStatus workerStatus = workerStatusMap.computeIfAbsent(workerId, k -> new WorkerStatus(workerId));
        workerStatus.setStatus(status);
        workerStatus.setJobExecutionId(jobExecutionId);
        workerStatus.setLastUpdated(LocalDateTime.now());

        if (status == BatchStatus.STARTED) {
            workerStatus.setStartTime(LocalDateTime.now());
            workerStatus.setEndTime(null);
        } else if (status == BatchStatus.COMPLETED || status == BatchStatus.FAILED || status == BatchStatus.STOPPED) {
            workerStatus.setEndTime(LocalDateTime.now());
        }

        log.info("Worker {} status updated to: {}", workerId, status);
    }

    public void updateProgress(int workerId, long processedCount, long totalCount) {
        WorkerStatus workerStatus = workerStatusMap.get(workerId);
        if (workerStatus != null) {
            workerStatus.setProcessedCount(processedCount);
            workerStatus.setTotalCount(totalCount);
            workerStatus.setLastUpdated(LocalDateTime.now());
        }
    }

    public WorkerStatus getWorkerStatus(int workerId) {
        return workerStatusMap.get(workerId);
    }

    public Map<Integer, WorkerStatus> getAllWorkerStatuses() {
        return new HashMap<>(workerStatusMap);
    }

    public boolean isWorkerRunning(int workerId) {
        WorkerStatus status = workerStatusMap.get(workerId);
        if (status == null) {
            return false;
        }

        // Check job explorer for more accurate status
        if (status.getJobExecutionId() != null) {
            JobExecution execution = jobExplorer.getJobExecution(status.getJobExecutionId());
            if (execution != null) {
                return execution.isRunning();
            }
        }

        return status.getStatus() == BatchStatus.STARTED || status.getStatus() == BatchStatus.STARTING;
    }

    public JobExecution getJobExecution(Long executionId) {
        return jobExplorer.getJobExecution(executionId);
    }

    public List<JobExecution> getRunningJobExecutions() {
        return jobExplorer.findRunningJobExecutions("crawlingJob")
                .stream()
                .toList();
    }

    public static class WorkerStatus {
        private final int workerId;
        private BatchStatus status;
        private Long jobExecutionId;
        private LocalDateTime startTime;
        private LocalDateTime endTime;
        private LocalDateTime lastUpdated;
        private long processedCount;
        private long totalCount;

        public WorkerStatus(int workerId) {
            this.workerId = workerId;
            this.status = BatchStatus.UNKNOWN;
            this.lastUpdated = LocalDateTime.now();
        }

        public int getWorkerId() {
            return workerId;
        }

        public BatchStatus getStatus() {
            return status;
        }

        public void setStatus(BatchStatus status) {
            this.status = status;
        }

        public Long getJobExecutionId() {
            return jobExecutionId;
        }

        public void setJobExecutionId(Long jobExecutionId) {
            this.jobExecutionId = jobExecutionId;
        }

        public LocalDateTime getStartTime() {
            return startTime;
        }

        public void setStartTime(LocalDateTime startTime) {
            this.startTime = startTime;
        }

        public LocalDateTime getEndTime() {
            return endTime;
        }

        public void setEndTime(LocalDateTime endTime) {
            this.endTime = endTime;
        }

        public LocalDateTime getLastUpdated() {
            return lastUpdated;
        }

        public void setLastUpdated(LocalDateTime lastUpdated) {
            this.lastUpdated = lastUpdated;
        }

        public long getProcessedCount() {
            return processedCount;
        }

        public void setProcessedCount(long processedCount) {
            this.processedCount = processedCount;
        }

        public long getTotalCount() {
            return totalCount;
        }

        public void setTotalCount(long totalCount) {
            this.totalCount = totalCount;
        }

        public double getProgressPercent() {
            if (totalCount == 0) return 0;
            return (processedCount * 100.0) / totalCount;
        }

        public Map<String, Object> toMap() {
            Map<String, Object> map = new HashMap<>();
            map.put("workerId", workerId);
            map.put("status", status.toString());
            map.put("jobExecutionId", jobExecutionId);
            map.put("startTime", startTime);
            map.put("endTime", endTime);
            map.put("lastUpdated", lastUpdated);
            map.put("processedCount", processedCount);
            map.put("totalCount", totalCount);
            map.put("progressPercent", String.format("%.2f", getProgressPercent()));
            return map;
        }
    }
}
