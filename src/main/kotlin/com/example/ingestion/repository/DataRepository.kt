package com.example.ingestion.repository

import com.example.ingestion.entity.DataEntity
import org.springframework.data.domain.Page
import org.springframework.data.domain.Pageable
import org.springframework.data.jpa.repository.JpaRepository
import org.springframework.data.jpa.repository.Modifying
import org.springframework.data.jpa.repository.Query
import org.springframework.data.repository.query.Param
import org.springframework.stereotype.Repository
import java.time.LocalDateTime
import java.time.OffsetDateTime
import java.util.*

@Repository
interface DataRepository : JpaRepository<DataEntity, Long> {
    
    /**
     * Find by external ID for upsert operations
     */
    fun findByExternalId(externalId: String): Optional<DataEntity>
    
    /**
     * Check if record exists by external ID
     */
    fun existsByExternalId(externalId: String): Boolean
    
    /**
     * Find records by email
     */
    fun findByEmailContainingIgnoreCase(email: String, pageable: Pageable): Page<DataEntity>
    
    /**
     * Find records by department
     */
    fun findByDepartmentContainingIgnoreCase(department: String?, pageable: Pageable): Page<DataEntity>
    
    /**
     * Find records by status
     */
    fun findByStatus(status: String?, pageable: Pageable): Page<DataEntity>
    
    /**
     * Find records updated after specific timestamp
     */
    fun findByExternalUpdatedAtAfter(timestamp: OffsetDateTime): List<DataEntity>
    
    /**
     * Custom upsert query using native SQL for better performance
     */
    @Modifying
    @Query(
        value = """
        INSERT INTO ingested_data (external_id, name, email, phone, department, status, external_created_at, external_updated_at, created_at, updated_at)
        VALUES (:externalId, :name, :email, :phone, :department, :status, :externalCreatedAt, :externalUpdatedAt, :createdAt, :updatedAt)
        ON CONFLICT (external_id) DO UPDATE SET
            name = EXCLUDED.name,
            email = EXCLUDED.email,
            phone = EXCLUDED.phone,
            department = EXCLUDED.department,
            status = EXCLUDED.status,
            external_created_at = EXCLUDED.external_created_at,
            external_updated_at = EXCLUDED.external_updated_at,
            updated_at = EXCLUDED.updated_at
        """,
        nativeQuery = true
    )
    fun upsertByExternalId(
        @Param("externalId") externalId: String,
        @Param("name") name: String,
        @Param("email") email: String,
        @Param("phone") phone: String?,
        @Param("department") department: String?,
        @Param("status") status: String?,
        @Param("externalCreatedAt") externalCreatedAt: OffsetDateTime,
        @Param("externalUpdatedAt") externalUpdatedAt: OffsetDateTime,
        @Param("createdAt") createdAt: LocalDateTime,
        @Param("updatedAt") updatedAt: LocalDateTime
    )
    
    /**
     * Count records by status for metrics
     */
    fun countByStatus(status: String?): Long
    
    /**
     * Get latest updated record timestamp for incremental sync
     */
    @Query("SELECT MAX(d.externalUpdatedAt) FROM DataEntity d")
    fun findLatestExternalUpdatedAt(): OffsetDateTime?
}