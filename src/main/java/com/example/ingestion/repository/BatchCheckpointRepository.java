package com.example.ingestion.repository;

import com.example.ingestion.entity.BatchCheckpoint;
import com.example.ingestion.entity.BatchCheckpoint.CheckpointStatus;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

/**
 * 배치 체크포인트 레포지토리
 */
@Repository
public interface BatchCheckpointRepository extends JpaRepository<BatchCheckpoint, Long> {

    /**
     * 배치명과 지역타입으로 체크포인트 조회
     */
    List<BatchCheckpoint> findByBatchNameAndRegionTypeOrderByRegionCode(String batchName, String regionType);

    /**
     * 배치명, 지역타입, 상태로 체크포인트 조회
     */
    List<BatchCheckpoint> findByBatchNameAndRegionTypeAndStatus(String batchName, String regionType, CheckpointStatus status);

    /**
     * 특정 배치의 다음 처리할 지역 조회 (PENDING 상태)
     */
    @Query("SELECT c FROM BatchCheckpoint c WHERE c.batchName = :batchName AND c.regionType = :regionType AND c.status = 'PENDING' ORDER BY c.regionCode LIMIT 1")
    Optional<BatchCheckpoint> findNextPendingRegion(@Param("batchName") String batchName, @Param("regionType") String regionType);

    /**
     * 특정 부모 코드의 하위 지역들 조회
     */
    List<BatchCheckpoint> findByBatchNameAndParentCodeOrderByRegionCode(String batchName, String parentCode);

    /**
     * 배치명과 지역 코드로 체크포인트 조회
     */
    Optional<BatchCheckpoint> findByBatchNameAndRegionTypeAndRegionCode(String batchName, String regionType, String regionCode);

    /**
     * 특정 배치의 상태별 개수 조회
     */
    @Query("SELECT c.status, COUNT(c) FROM BatchCheckpoint c WHERE c.batchName = :batchName GROUP BY c.status")
    List<Object[]> countByBatchNameGroupByStatus(@Param("batchName") String batchName);

    /**
     * 진행률 계산을 위한 통계
     */
    @Query("SELECT " +
           "COUNT(*) as total, " +
           "SUM(CASE WHEN c.status = 'COMPLETED' THEN 1 ELSE 0 END) as completed, " +
           "SUM(CASE WHEN c.status = 'FAILED' THEN 1 ELSE 0 END) as failed, " +
           "SUM(CASE WHEN c.status = 'PROCESSING' THEN 1 ELSE 0 END) as processing " +
           "FROM BatchCheckpoint c WHERE c.batchName = :batchName")
    Object[] getBatchProgress(@Param("batchName") String batchName);

    /**
     * 마지막으로 완료된 체크포인트 조회
     */
    @Query("SELECT c FROM BatchCheckpoint c WHERE c.batchName = :batchName AND c.status = 'COMPLETED' ORDER BY c.updatedAt DESC LIMIT 1")
    Optional<BatchCheckpoint> findLastCompletedCheckpoint(@Param("batchName") String batchName);

    /**
     * 실행 중인 체크포인트 조회 (PROCESSING 상태)
     */
    List<BatchCheckpoint> findByBatchNameAndStatus(String batchName, CheckpointStatus status);
}