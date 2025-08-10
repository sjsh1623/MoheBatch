package com.example.ingestion.entity

import jakarta.persistence.*
import java.time.LocalDateTime
import java.time.OffsetDateTime

@Entity
@Table(
    name = "job_execution_state",
    indexes = [
        Index(name = "idx_job_name", columnList = "job_name", unique = true),
        Index(name = "idx_updated_at", columnList = "updated_at")
    ]
)
data class JobExecutionState(
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    val id: Long? = null,
    
    @Column(name = "job_name", nullable = false, unique = true, length = 100)
    val jobName: String,
    
    @Column(name = "last_processed_page", nullable = false)
    val lastProcessedPage: Int = 0,
    
    @Column(name = "last_processed_timestamp")
    val lastProcessedTimestamp: OffsetDateTime? = null,
    
    @Column(name = "total_processed_records", nullable = false)
    val totalProcessedRecords: Long = 0,
    
    @Column(name = "last_execution_status", length = 50)
    val lastExecutionStatus: String? = null,
    
    @Column(name = "created_at", nullable = false)
    val createdAt: LocalDateTime = LocalDateTime.now(),
    
    @Column(name = "updated_at", nullable = false)
    val updatedAt: LocalDateTime = LocalDateTime.now()
) {
    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false
        other as JobExecutionState
        return jobName == other.jobName
    }
    
    override fun hashCode(): Int {
        return jobName.hashCode()
    }
}