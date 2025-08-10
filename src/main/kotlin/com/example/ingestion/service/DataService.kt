package com.example.ingestion.service

import com.example.ingestion.dto.DataReadDto
import com.example.ingestion.dto.DataWriteDto
import com.example.ingestion.dto.PageInfo
import com.example.ingestion.dto.PagedDataResponse
import com.example.ingestion.entity.DataEntity
import com.example.ingestion.exception.ResourceNotFoundException
import com.example.ingestion.repository.DataRepository
import io.micrometer.core.instrument.MeterRegistry
import org.slf4j.LoggerFactory
import org.springframework.data.domain.PageRequest
import org.springframework.data.domain.Pageable
import org.springframework.data.domain.Sort
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional
import java.time.LocalDateTime
import java.time.OffsetDateTime

@Service
@Transactional(readOnly = true)
class DataService(
    private val dataRepository: DataRepository,
    private val meterRegistry: MeterRegistry
) {

    private val logger = LoggerFactory.getLogger(DataService::class.java)

    fun getAllData(
        page: Int = 0,
        size: Int = 20,
        sortBy: String = "updatedAt",
        sortDirection: String = "desc"
    ): PagedDataResponse {
        logger.debug("Fetching data page: {}, size: {}, sort: {} {}", page, size, sortBy, sortDirection)
        
        val sort = Sort.by(
            if (sortDirection.equals("desc", ignoreCase = true)) Sort.Direction.DESC else Sort.Direction.ASC,
            sortBy
        )
        
        val pageable: Pageable = PageRequest.of(page, size, sort)
        val result = dataRepository.findAll(pageable)
        
        meterRegistry.counter("data_service_get_all").increment()
        
        return PagedDataResponse(
            data = result.content.map { DataReadDto.fromEntity(it) },
            pagination = PageInfo(
                page = result.number,
                size = result.size,
                totalPages = result.totalPages,
                totalElements = result.totalElements,
                hasNext = result.hasNext(),
                hasPrevious = result.hasPrevious()
            )
        )
    }

    fun getDataById(id: Long): DataReadDto {
        logger.debug("Fetching data by ID: {}", id)
        
        val entity = dataRepository.findById(id)
            .orElseThrow { ResourceNotFoundException("Data not found with ID: $id") }
            
        meterRegistry.counter("data_service_get_by_id").increment()
        
        return DataReadDto.fromEntity(entity)
    }

    fun getDataByExternalId(externalId: String): DataReadDto {
        logger.debug("Fetching data by external ID: {}", externalId)
        
        val entity = dataRepository.findByExternalId(externalId)
            .orElseThrow { ResourceNotFoundException("Data not found with external ID: $externalId") }
            
        meterRegistry.counter("data_service_get_by_external_id").increment()
        
        return DataReadDto.fromEntity(entity)
    }

    fun searchByEmail(email: String, page: Int = 0, size: Int = 20): PagedDataResponse {
        logger.debug("Searching data by email: {}", email)
        
        val pageable: Pageable = PageRequest.of(page, size, Sort.by(Sort.Direction.DESC, "updatedAt"))
        val result = dataRepository.findByEmailContainingIgnoreCase(email, pageable)
        
        meterRegistry.counter("data_service_search_by_email").increment()
        
        return PagedDataResponse(
            data = result.content.map { DataReadDto.fromEntity(it) },
            pagination = PageInfo(
                page = result.number,
                size = result.size,
                totalPages = result.totalPages,
                totalElements = result.totalElements,
                hasNext = result.hasNext(),
                hasPrevious = result.hasPrevious()
            )
        )
    }

    fun searchByDepartment(department: String, page: Int = 0, size: Int = 20): PagedDataResponse {
        logger.debug("Searching data by department: {}", department)
        
        val pageable: Pageable = PageRequest.of(page, size, Sort.by(Sort.Direction.DESC, "updatedAt"))
        val result = dataRepository.findByDepartmentContainingIgnoreCase(department, pageable)
        
        meterRegistry.counter("data_service_search_by_department").increment()
        
        return PagedDataResponse(
            data = result.content.map { DataReadDto.fromEntity(it) },
            pagination = PageInfo(
                page = result.number,
                size = result.size,
                totalPages = result.totalPages,
                totalElements = result.totalElements,
                hasNext = result.hasNext(),
                hasPrevious = result.hasPrevious()
            )
        )
    }

    fun getDataByStatus(status: String, page: Int = 0, size: Int = 20): PagedDataResponse {
        logger.debug("Fetching data by status: {}", status)
        
        val pageable: Pageable = PageRequest.of(page, size, Sort.by(Sort.Direction.DESC, "updatedAt"))
        val result = dataRepository.findByStatus(status, pageable)
        
        meterRegistry.counter("data_service_get_by_status").increment()
        
        return PagedDataResponse(
            data = result.content.map { DataReadDto.fromEntity(it) },
            pagination = PageInfo(
                page = result.number,
                size = result.size,
                totalPages = result.totalPages,
                totalElements = result.totalElements,
                hasNext = result.hasNext(),
                hasPrevious = result.hasPrevious()
            )
        )
    }

    @Transactional
    fun createOrUpdateData(writeDto: DataWriteDto): DataReadDto {
        logger.debug("Creating or updating data with external ID: {}", writeDto.externalId)
        
        val now = LocalDateTime.now()
        val entity = DataEntity(
            externalId = writeDto.externalId,
            name = writeDto.name,
            email = writeDto.email,
            phone = writeDto.phone,
            department = writeDto.department,
            status = writeDto.status,
            externalCreatedAt = writeDto.externalCreatedAt,
            externalUpdatedAt = writeDto.externalUpdatedAt,
            createdAt = now,
            updatedAt = now
        )

        // Use the upsert method for idempotency
        dataRepository.upsertByExternalId(
            externalId = entity.externalId,
            name = entity.name,
            email = entity.email,
            phone = entity.phone,
            department = entity.department,
            status = entity.status,
            externalCreatedAt = entity.externalCreatedAt,
            externalUpdatedAt = entity.externalUpdatedAt,
            createdAt = entity.createdAt,
            updatedAt = entity.updatedAt
        )

        meterRegistry.counter("data_service_create_or_update").increment()
        
        // Fetch and return the saved entity
        return getDataByExternalId(writeDto.externalId)
    }

    fun getDataSummary(): Map<String, Any> {
        logger.debug("Fetching data summary statistics")
        
        val totalCount = dataRepository.count()
        val activeCount = dataRepository.countByStatus("active")
        val inactiveCount = dataRepository.countByStatus("inactive")
        val pendingCount = dataRepository.countByStatus("pending")
        val latestUpdateTimestamp = dataRepository.findLatestExternalUpdatedAt()
        
        meterRegistry.counter("data_service_get_summary").increment()
        
        return mapOf(
            "total_count" to totalCount,
            "active_count" to activeCount,
            "inactive_count" to inactiveCount,
            "pending_count" to pendingCount,
            "latest_update_timestamp" to latestUpdateTimestamp
        )
    }

    fun getDataUpdatedAfter(timestamp: OffsetDateTime): List<DataReadDto> {
        logger.debug("Fetching data updated after: {}", timestamp)
        
        val entities = dataRepository.findByExternalUpdatedAtAfter(timestamp)
        
        meterRegistry.counter("data_service_get_updated_after").increment()
        
        return entities.map { DataReadDto.fromEntity(it) }
    }
}