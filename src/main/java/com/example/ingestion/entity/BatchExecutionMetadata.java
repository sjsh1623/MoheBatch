package com.example.ingestion.entity;

import jakarta.persistence.*;
import org.hibernate.annotations.CreationTimestamp;
import org.hibernate.annotations.UpdateTimestamp;

import java.time.ZonedDateTime;

/**
 * 배치 실행 메타데이터 엔티티
 * 전체 배치 실행 상태 및 통계 추적
 */
@Entity
@Table(name = "batch_execution_metadata")
public class BatchExecutionMetadata {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "batch_name", nullable = false, length = 100)
    private String batchName;

    @Column(name = "execution_id", nullable = false, length = 50)
    private String executionId;

    @Column(name = "start_time", nullable = false)
    private ZonedDateTime startTime;

    @Column(name = "end_time")
    private ZonedDateTime endTime;

    @Enumerated(EnumType.STRING)
    @Column(name = "status", nullable = false, length = 20)
    private ExecutionStatus status = ExecutionStatus.RUNNING;

    @Column(name = "total_regions")
    private Integer totalRegions = 0;

    @Column(name = "completed_regions")
    private Integer completedRegions = 0;

    @Column(name = "failed_regions")
    private Integer failedRegions = 0;

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "last_checkpoint_id")
    private BatchCheckpoint lastCheckpoint;

    @CreationTimestamp
    @Column(name = "created_at", nullable = false, updatable = false)
    private ZonedDateTime createdAt;

    @UpdateTimestamp
    @Column(name = "updated_at", nullable = false)
    private ZonedDateTime updatedAt;

    public enum ExecutionStatus {
        RUNNING, COMPLETED, FAILED, INTERRUPTED
    }

    // Constructors
    public BatchExecutionMetadata() {}

    public BatchExecutionMetadata(String batchName, String executionId) {
        this.batchName = batchName;
        this.executionId = executionId;
        this.startTime = ZonedDateTime.now();
    }

    // Getters and Setters
    public Long getId() { return id; }
    public void setId(Long id) { this.id = id; }

    public String getBatchName() { return batchName; }
    public void setBatchName(String batchName) { this.batchName = batchName; }

    public String getExecutionId() { return executionId; }
    public void setExecutionId(String executionId) { this.executionId = executionId; }

    public ZonedDateTime getStartTime() { return startTime; }
    public void setStartTime(ZonedDateTime startTime) { this.startTime = startTime; }

    public ZonedDateTime getEndTime() { return endTime; }
    public void setEndTime(ZonedDateTime endTime) { this.endTime = endTime; }

    public ExecutionStatus getStatus() { return status; }
    public void setStatus(ExecutionStatus status) { this.status = status; }

    public Integer getTotalRegions() { return totalRegions; }
    public void setTotalRegions(Integer totalRegions) { this.totalRegions = totalRegions; }

    public Integer getCompletedRegions() { return completedRegions; }
    public void setCompletedRegions(Integer completedRegions) { this.completedRegions = completedRegions; }

    public Integer getFailedRegions() { return failedRegions; }
    public void setFailedRegions(Integer failedRegions) { this.failedRegions = failedRegions; }

    public BatchCheckpoint getLastCheckpoint() { return lastCheckpoint; }
    public void setLastCheckpoint(BatchCheckpoint lastCheckpoint) { this.lastCheckpoint = lastCheckpoint; }

    public ZonedDateTime getCreatedAt() { return createdAt; }
    public void setCreatedAt(ZonedDateTime createdAt) { this.createdAt = createdAt; }

    public ZonedDateTime getUpdatedAt() { return updatedAt; }
    public void setUpdatedAt(ZonedDateTime updatedAt) { this.updatedAt = updatedAt; }

    // Helper methods
    public void incrementCompleted() {
        this.completedRegions++;
    }

    public void incrementFailed() {
        this.failedRegions++;
    }

    public double getCompletionPercentage() {
        if (totalRegions == 0) return 0.0;
        return (double) completedRegions / totalRegions * 100.0;
    }

    public void markAsCompleted() {
        this.status = ExecutionStatus.COMPLETED;
        this.endTime = ZonedDateTime.now();
    }

    public void markAsFailed() {
        this.status = ExecutionStatus.FAILED;
        this.endTime = ZonedDateTime.now();
    }

    public void markAsInterrupted() {
        this.status = ExecutionStatus.INTERRUPTED;
        this.endTime = ZonedDateTime.now();
    }

    @Override
    public String toString() {
        return String.format("BatchExecutionMetadata{id=%d, batchName='%s', executionId='%s', status=%s, progress=%.1f%%}",
                id, batchName, executionId, status, getCompletionPercentage());
    }
}