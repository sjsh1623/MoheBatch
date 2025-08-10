package com.example.ingestion.repository

import com.example.ingestion.entity.JobExecutionState
import org.springframework.data.jpa.repository.JpaRepository
import org.springframework.data.jpa.repository.Modifying
import org.springframework.data.jpa.repository.Query
import org.springframework.data.repository.query.Param
import org.springframework.stereotype.Repository
import java.time.LocalDateTime
import java.time.OffsetDateTime
import java.util.*

@Repository
interface JobExecutionStateRepository : JpaRepository<JobExecutionState, Long> {
    
    /**
     * Find job execution state by job name
     */
    fun findByJobName(jobName: String): Optional<JobExecutionState>
    
    /**
     * Upsert job execution state
     */
    @Modifying
    @Query(
        value = """
        INSERT INTO job_execution_state (job_name, last_processed_page, last_processed_timestamp, total_processed_records, last_execution_status, created_at, updated_at)
        VALUES (:jobName, :lastProcessedPage, :lastProcessedTimestamp, :totalProcessedRecords, :lastExecutionStatus, :createdAt, :updatedAt)
        ON CONFLICT (job_name) DO UPDATE SET
            last_processed_page = EXCLUDED.last_processed_page,
            last_processed_timestamp = EXCLUDED.last_processed_timestamp,
            total_processed_records = EXCLUDED.total_processed_records,
            last_execution_status = EXCLUDED.last_execution_status,
            updated_at = EXCLUDED.updated_at
        """,
        nativeQuery = true
    )
    fun upsertByJobName(
        @Param("jobName") jobName: String,
        @Param("lastProcessedPage") lastProcessedPage: Int,
        @Param("lastProcessedTimestamp") lastProcessedTimestamp: OffsetDateTime?,
        @Param("totalProcessedRecords") totalProcessedRecords: Long,
        @Param("lastExecutionStatus") lastExecutionStatus: String?,
        @Param("createdAt") createdAt: LocalDateTime,
        @Param("updatedAt") updatedAt: LocalDateTime
    )
}