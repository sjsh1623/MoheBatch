package com.example.ingestion.repository;

import com.example.ingestion.entity.BatchExecutionMetadata;
import com.example.ingestion.entity.BatchExecutionMetadata.ExecutionStatus;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

/**
 * 배치 실행 메타데이터 레포지토리
 */
@Repository
public interface BatchExecutionMetadataRepository extends JpaRepository<BatchExecutionMetadata, Long> {

    /**
     * 배치명으로 실행 메타데이터 조회 (최신순)
     */
    List<BatchExecutionMetadata> findByBatchNameOrderByStartTimeDesc(String batchName);

    /**
     * 실행 ID로 조회
     */
    Optional<BatchExecutionMetadata> findByExecutionId(String executionId);

    /**
     * 현재 실행 중인 배치 조회
     */
    Optional<BatchExecutionMetadata> findByBatchNameAndStatus(String batchName, ExecutionStatus status);

    /**
     * 마지막 실행 메타데이터 조회
     */
    @Query("SELECT m FROM BatchExecutionMetadata m WHERE m.batchName = :batchName ORDER BY m.startTime DESC LIMIT 1")
    Optional<BatchExecutionMetadata> findLastExecution(@Param("batchName") String batchName);

    /**
     * 중단된 배치 조회 (INTERRUPTED 상태)
     */
    List<BatchExecutionMetadata> findByStatusOrderByStartTimeDesc(ExecutionStatus status);

    /**
     * 특정 기간 내 실행 통계
     */
    @Query("SELECT " +
           "COUNT(*) as totalExecutions, " +
           "SUM(CASE WHEN m.status = 'COMPLETED' THEN 1 ELSE 0 END) as completed, " +
           "SUM(CASE WHEN m.status = 'FAILED' THEN 1 ELSE 0 END) as failed, " +
           "SUM(CASE WHEN m.status = 'INTERRUPTED' THEN 1 ELSE 0 END) as interrupted " +
           "FROM BatchExecutionMetadata m WHERE m.batchName = :batchName")
    Object[] getExecutionStatistics(@Param("batchName") String batchName);
}