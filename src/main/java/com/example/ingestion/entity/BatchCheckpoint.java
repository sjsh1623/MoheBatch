package com.example.ingestion.entity;

import jakarta.persistence.*;
import org.hibernate.annotations.CreationTimestamp;
import org.hibernate.annotations.UpdateTimestamp;

import java.time.ZonedDateTime;

/**
 * 배치 체크포인트 엔티티
 * 정부 API 기반 진행 상태 추적
 */
@Entity
@Table(name = "batch_checkpoint",
       uniqueConstraints = @UniqueConstraint(columnNames = {"batch_name", "region_type", "region_code"}))
public class BatchCheckpoint {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "batch_name", nullable = false, length = 100)
    private String batchName;

    @Column(name = "region_type", nullable = false, length = 50)
    private String regionType; // 'sido', 'sigungu', 'dong'

    @Column(name = "region_code", nullable = false, length = 20)
    private String regionCode;

    @Column(name = "region_name", nullable = false, length = 200)
    private String regionName;

    @Column(name = "parent_code", length = 20)
    private String parentCode;

    @Enumerated(EnumType.STRING)
    @Column(name = "status", nullable = false, length = 20)
    private CheckpointStatus status = CheckpointStatus.PENDING;

    @Column(name = "start_time")
    private ZonedDateTime startTime;

    @Column(name = "end_time")
    private ZonedDateTime endTime;

    @Column(name = "processed_count")
    private Integer processedCount = 0;

    @Column(name = "error_message", columnDefinition = "TEXT")
    private String errorMessage;

    @CreationTimestamp
    @Column(name = "created_at", nullable = false, updatable = false)
    private ZonedDateTime createdAt;

    @UpdateTimestamp
    @Column(name = "updated_at", nullable = false)
    private ZonedDateTime updatedAt;

    public enum CheckpointStatus {
        PENDING, PROCESSING, COMPLETED, FAILED
    }

    // Constructors
    public BatchCheckpoint() {}

    public BatchCheckpoint(String batchName, String regionType, String regionCode, String regionName) {
        this.batchName = batchName;
        this.regionType = regionType;
        this.regionCode = regionCode;
        this.regionName = regionName;
    }

    // Getters and Setters
    public Long getId() { return id; }
    public void setId(Long id) { this.id = id; }

    public String getBatchName() { return batchName; }
    public void setBatchName(String batchName) { this.batchName = batchName; }

    public String getRegionType() { return regionType; }
    public void setRegionType(String regionType) { this.regionType = regionType; }

    public String getRegionCode() { return regionCode; }
    public void setRegionCode(String regionCode) { this.regionCode = regionCode; }

    public String getRegionName() { return regionName; }
    public void setRegionName(String regionName) { this.regionName = regionName; }

    public String getParentCode() { return parentCode; }
    public void setParentCode(String parentCode) { this.parentCode = parentCode; }

    public CheckpointStatus getStatus() { return status; }
    public void setStatus(CheckpointStatus status) { this.status = status; }

    public ZonedDateTime getStartTime() { return startTime; }
    public void setStartTime(ZonedDateTime startTime) { this.startTime = startTime; }

    public ZonedDateTime getEndTime() { return endTime; }
    public void setEndTime(ZonedDateTime endTime) { this.endTime = endTime; }

    public Integer getProcessedCount() { return processedCount; }
    public void setProcessedCount(Integer processedCount) { this.processedCount = processedCount; }

    public String getErrorMessage() { return errorMessage; }
    public void setErrorMessage(String errorMessage) { this.errorMessage = errorMessage; }

    public ZonedDateTime getCreatedAt() { return createdAt; }
    public void setCreatedAt(ZonedDateTime createdAt) { this.createdAt = createdAt; }

    public ZonedDateTime getUpdatedAt() { return updatedAt; }
    public void setUpdatedAt(ZonedDateTime updatedAt) { this.updatedAt = updatedAt; }

    // Helper methods
    public void markAsProcessing() {
        this.status = CheckpointStatus.PROCESSING;
        this.startTime = ZonedDateTime.now();
    }

    public void markAsCompleted(int processedCount) {
        this.status = CheckpointStatus.COMPLETED;
        this.endTime = ZonedDateTime.now();
        this.processedCount = processedCount;
        this.errorMessage = null;
    }

    public void markAsFailed(String errorMessage) {
        this.status = CheckpointStatus.FAILED;
        this.endTime = ZonedDateTime.now();
        this.errorMessage = errorMessage;
    }

    @Override
    public String toString() {
        return String.format("BatchCheckpoint{id=%d, regionType='%s', regionCode='%s', regionName='%s', status=%s}",
                id, regionType, regionCode, regionName, status);
    }
}