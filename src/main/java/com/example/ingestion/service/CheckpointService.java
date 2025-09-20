package com.example.ingestion.service;

import com.example.ingestion.entity.BatchCheckpoint;
import com.example.ingestion.entity.BatchCheckpoint.CheckpointStatus;
import com.example.ingestion.entity.BatchExecutionMetadata;
import com.example.ingestion.entity.BatchExecutionMetadata.ExecutionStatus;
import com.example.ingestion.repository.BatchCheckpointRepository;
import com.example.ingestion.repository.BatchExecutionMetadataRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.ZonedDateTime;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

/**
 * 배치 체크포인트 서비스
 * 정부 API 기반 진행 상태 추적 및 중단점 복구 관리
 */
@Service
@Transactional
public class CheckpointService {

    private static final Logger logger = LoggerFactory.getLogger(CheckpointService.class);

    @Autowired
    private BatchCheckpointRepository checkpointRepository;

    @Autowired
    private BatchExecutionMetadataRepository executionMetadataRepository;

    /**
     * 새로운 배치 실행 시작
     */
    public BatchExecutionMetadata startBatchExecution(String batchName) {
        String executionId = UUID.randomUUID().toString();

        // 기존 실행 중인 배치가 있다면 중단으로 마킹
        Optional<BatchExecutionMetadata> runningBatch =
            executionMetadataRepository.findByBatchNameAndStatus(batchName, ExecutionStatus.RUNNING);

        if (runningBatch.isPresent()) {
            logger.warn("🔄 기존 실행 중인 배치 발견, 중단으로 마킹: {}", runningBatch.get().getExecutionId());
            runningBatch.get().markAsInterrupted();
            executionMetadataRepository.save(runningBatch.get());
        }

        BatchExecutionMetadata metadata = new BatchExecutionMetadata(batchName, executionId);
        metadata = executionMetadataRepository.save(metadata);

        logger.info("🚀 새 배치 실행 시작: {} (ID: {})", batchName, executionId);
        return metadata;
    }

    /**
     * 지역 체크포인트 초기화 (정부 API에서 가져온 지역 목록)
     */
    public void initializeRegionCheckpoints(String batchName, String regionType, List<RegionInfo> regions) {
        logger.info("📍 지역 체크포인트 초기화: {} - {} ({} regions)", batchName, regionType, regions.size());

        for (RegionInfo region : regions) {
            Optional<BatchCheckpoint> existing = checkpointRepository
                .findByBatchNameAndRegionTypeAndRegionCode(batchName, regionType, region.getCode());

            if (existing.isEmpty()) {
                BatchCheckpoint checkpoint = new BatchCheckpoint(batchName, regionType, region.getCode(), region.getName());
                checkpoint.setParentCode(region.getParentCode());
                checkpointRepository.save(checkpoint);
                logger.debug("  ➕ 새 체크포인트 생성: {} ({})", region.getName(), region.getCode());
            } else {
                // 기존 체크포인트가 FAILED 상태라면 PENDING으로 리셋
                if (existing.get().getStatus() == CheckpointStatus.FAILED) {
                    existing.get().setStatus(CheckpointStatus.PENDING);
                    existing.get().setErrorMessage(null);
                    checkpointRepository.save(existing.get());
                    logger.debug("  🔄 실패한 체크포인트 리셋: {} ({})", region.getName(), region.getCode());
                }
            }
        }

        // 실행 메타데이터 업데이트
        updateExecutionProgress(batchName, regionType, regions.size());
    }

    /**
     * 다음 처리할 지역 조회 (체크포인트 기반 재시작)
     */
    public Optional<BatchCheckpoint> getNextPendingRegion(String batchName, String regionType) {
        Optional<BatchCheckpoint> next = checkpointRepository.findNextPendingRegion(batchName, regionType);

        if (next.isPresent()) {
            logger.debug("📌 다음 처리 지역: {} ({})", next.get().getRegionName(), next.get().getRegionCode());
        } else {
            logger.info("✅ 모든 {} 지역 처리 완료", regionType);
        }

        return next;
    }

    /**
     * 지역 처리 시작 마킹
     */
    public void markRegionAsProcessing(Long checkpointId) {
        Optional<BatchCheckpoint> checkpoint = checkpointRepository.findById(checkpointId);
        if (checkpoint.isPresent()) {
            checkpoint.get().markAsProcessing();
            checkpointRepository.save(checkpoint.get());
            logger.debug("🔄 지역 처리 시작: {} ({})",
                        checkpoint.get().getRegionName(), checkpoint.get().getRegionCode());
        }
    }

    /**
     * 지역 처리 완료 마킹
     */
    public void markRegionAsCompleted(Long checkpointId, int processedCount) {
        Optional<BatchCheckpoint> checkpoint = checkpointRepository.findById(checkpointId);
        if (checkpoint.isPresent()) {
            checkpoint.get().markAsCompleted(processedCount);
            BatchCheckpoint saved = checkpointRepository.save(checkpoint.get());

            logger.info("✅ 지역 처리 완료: {} ({}) - {} places processed",
                       saved.getRegionName(), saved.getRegionCode(), processedCount);

            // 실행 메타데이터 업데이트
            updateExecutionMetadata(saved.getBatchName(), saved);
        }
    }

    /**
     * 지역 처리 실패 마킹
     */
    public void markRegionAsFailed(Long checkpointId, String errorMessage) {
        Optional<BatchCheckpoint> checkpoint = checkpointRepository.findById(checkpointId);
        if (checkpoint.isPresent()) {
            checkpoint.get().markAsFailed(errorMessage);
            BatchCheckpoint saved = checkpointRepository.save(checkpoint.get());

            logger.error("❌ 지역 처리 실패: {} ({}) - {}",
                        saved.getRegionName(), saved.getRegionCode(), errorMessage);

            // 실행 메타데이터 업데이트
            updateExecutionMetadata(saved.getBatchName(), saved);
        }
    }

    /**
     * 배치 실행 완료 처리
     */
    public void completeBatchExecution(String batchName) {
        Optional<BatchExecutionMetadata> metadata =
            executionMetadataRepository.findByBatchNameAndStatus(batchName, ExecutionStatus.RUNNING);

        if (metadata.isPresent()) {
            metadata.get().markAsCompleted();
            executionMetadataRepository.save(metadata.get());
            logger.info("🎉 배치 실행 완료: {}", batchName);
        }
    }

    /**
     * 배치 실행 실패 처리
     */
    public void failBatchExecution(String batchName) {
        Optional<BatchExecutionMetadata> metadata =
            executionMetadataRepository.findByBatchNameAndStatus(batchName, ExecutionStatus.RUNNING);

        if (metadata.isPresent()) {
            metadata.get().markAsFailed();
            executionMetadataRepository.save(metadata.get());
            logger.error("💥 배치 실행 실패: {}", batchName);
        }
    }

    /**
     * 중단된 배치가 있는지 확인
     */
    public boolean hasInterruptedBatch(String batchName) {
        List<BatchExecutionMetadata> interrupted =
            executionMetadataRepository.findByStatusOrderByStartTimeDesc(ExecutionStatus.INTERRUPTED);

        return interrupted.stream().anyMatch(meta -> meta.getBatchName().equals(batchName));
    }

    /**
     * 진행 상태 조회
     */
    @Transactional(readOnly = true)
    public BatchProgress getBatchProgress(String batchName) {
        try {
            Object[] stats = checkpointRepository.getBatchProgress(batchName);

            if (stats != null && stats.length >= 4) {
                long total = stats[0] != null ? ((Number) stats[0]).longValue() : 0;
                long completed = stats[1] != null ? ((Number) stats[1]).longValue() : 0;
                long failed = stats[2] != null ? ((Number) stats[2]).longValue() : 0;
                long processing = stats[3] != null ? ((Number) stats[3]).longValue() : 0;

                return new BatchProgress(total, completed, failed, processing);
            }
        } catch (Exception e) {
            logger.warn("⚠️ 배치 진행 상태 조회 실패: {}", e.getMessage());
        }

        return new BatchProgress(0, 0, 0, 0);
    }

    /**
     * 실행 메타데이터 업데이트
     */
    private void updateExecutionMetadata(String batchName, BatchCheckpoint checkpoint) {
        Optional<BatchExecutionMetadata> metadata =
            executionMetadataRepository.findByBatchNameAndStatus(batchName, ExecutionStatus.RUNNING);

        if (metadata.isPresent()) {
            if (checkpoint.getStatus() == CheckpointStatus.COMPLETED) {
                metadata.get().incrementCompleted();
            } else if (checkpoint.getStatus() == CheckpointStatus.FAILED) {
                metadata.get().incrementFailed();
            }

            metadata.get().setLastCheckpoint(checkpoint);
            executionMetadataRepository.save(metadata.get());
        }
    }

    /**
     * 실행 진행률 업데이트
     */
    private void updateExecutionProgress(String batchName, String regionType, int totalRegions) {
        Optional<BatchExecutionMetadata> metadata =
            executionMetadataRepository.findByBatchNameAndStatus(batchName, ExecutionStatus.RUNNING);

        if (metadata.isPresent()) {
            metadata.get().setTotalRegions(totalRegions);
            executionMetadataRepository.save(metadata.get());
        }
    }

    /**
     * 지역 정보 DTO
     */
    public static class RegionInfo {
        private String code;
        private String name;
        private String parentCode;

        public RegionInfo(String code, String name, String parentCode) {
            this.code = code;
            this.name = name;
            this.parentCode = parentCode;
        }

        public String getCode() { return code; }
        public String getName() { return name; }
        public String getParentCode() { return parentCode; }
    }

    /**
     * 배치 진행 상태 DTO
     */
    public static class BatchProgress {
        private final long total;
        private final long completed;
        private final long failed;
        private final long processing;

        public BatchProgress(long total, long completed, long failed, long processing) {
            this.total = total;
            this.completed = completed;
            this.failed = failed;
            this.processing = processing;
        }

        public long getTotal() { return total; }
        public long getCompleted() { return completed; }
        public long getFailed() { return failed; }
        public long getProcessing() { return processing; }
        public long getPending() { return total - completed - failed - processing; }
        public double getCompletionPercentage() {
            return total > 0 ? (double) completed / total * 100.0 : 0.0;
        }

        @Override
        public String toString() {
            return String.format("Progress{total=%d, completed=%d, failed=%d, processing=%d, %.1f%%}",
                               total, completed, failed, processing, getCompletionPercentage());
        }
    }
}