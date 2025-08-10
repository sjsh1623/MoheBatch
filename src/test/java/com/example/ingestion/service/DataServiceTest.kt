package com.example.ingestion.service

import com.example.ingestion.dto.DataWriteDto
import com.example.ingestion.entity.DataEntity
import com.example.ingestion.exception.ResourceNotFoundException
import com.example.ingestion.repository.DataRepository
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import org.mockito.Mock
import org.mockito.junit.jupiter.MockitoExtension
import org.mockito.kotlin.*
import org.springframework.data.domain.*
import java.time.LocalDateTime
import java.time.OffsetDateTime
import java.util.*

@ExtendWith(MockitoExtension::class)
class DataServiceTest {

    @Mock
    private lateinit var dataRepository: DataRepository

    private lateinit var dataService: DataService
    private lateinit var meterRegistry: SimpleMeterRegistry

    @BeforeEach
    fun setUp() {
        meterRegistry = SimpleMeterRegistry()
        dataService = DataService(dataRepository, meterRegistry)
    }

    @Test
    fun `getAllData should return paginated results`() {
        // Given
        val entities = listOf(createDataEntity("ext-123"), createDataEntity("ext-456"))
        val page = PageImpl(entities, PageRequest.of(0, 20), 2)

        whenever(dataRepository.findAll(any<Pageable>())).thenReturn(page)

        // When
        val result = dataService.getAllData(0, 20, "updatedAt", "desc")

        // Then
        assertNotNull(result)
        assertEquals(2, result.data.size)
        assertEquals(0, result.pagination.page)
        assertEquals(20, result.pagination.size)
        assertEquals(1, result.pagination.totalPages)
        assertEquals(2, result.pagination.totalElements)
        assertFalse(result.pagination.hasNext)
        assertFalse(result.pagination.hasPrevious)

        assertEquals(1.0, meterRegistry.counter("data_service_get_all").count())
    }

    @Test
    fun `getDataById should return data when found`() {
        // Given
        val entity = createDataEntity("ext-123")
        whenever(dataRepository.findById(1L)).thenReturn(Optional.of(entity))

        // When
        val result = dataService.getDataById(1L)

        // Then
        assertNotNull(result)
        assertEquals("ext-123", result.externalId)
        assertEquals("John Doe", result.name)
        assertEquals("john.doe@example.com", result.email)

        assertEquals(1.0, meterRegistry.counter("data_service_get_by_id").count())
    }

    @Test
    fun `getDataById should throw exception when not found`() {
        // Given
        whenever(dataRepository.findById(1L)).thenReturn(Optional.empty())

        // When & Then
        val exception = assertThrows<ResourceNotFoundException> {
            dataService.getDataById(1L)
        }

        assertEquals("Data not found with ID: 1", exception.message)
    }

    @Test
    fun `getDataByExternalId should return data when found`() {
        // Given
        val entity = createDataEntity("ext-123")
        whenever(dataRepository.findByExternalId("ext-123")).thenReturn(Optional.of(entity))

        // When
        val result = dataService.getDataByExternalId("ext-123")

        // Then
        assertNotNull(result)
        assertEquals("ext-123", result.externalId)
        assertEquals(1.0, meterRegistry.counter("data_service_get_by_external_id").count())
    }

    @Test
    fun `getDataByExternalId should throw exception when not found`() {
        // Given
        whenever(dataRepository.findByExternalId("ext-999")).thenReturn(Optional.empty())

        // When & Then
        val exception = assertThrows<ResourceNotFoundException> {
            dataService.getDataByExternalId("ext-999")
        }

        assertEquals("Data not found with external ID: ext-999", exception.message)
    }

    @Test
    fun `searchByEmail should return paginated results`() {
        // Given
        val entities = listOf(createDataEntity("ext-123"))
        val page = PageImpl(entities, PageRequest.of(0, 20), 1)

        whenever(dataRepository.findByEmailContainingIgnoreCase(eq("john"), any<Pageable>()))
            .thenReturn(page)

        // When
        val result = dataService.searchByEmail("john", 0, 20)

        // Then
        assertNotNull(result)
        assertEquals(1, result.data.size)
        assertEquals(1.0, meterRegistry.counter("data_service_search_by_email").count())
    }

    @Test
    fun `searchByDepartment should return paginated results`() {
        // Given
        val entities = listOf(createDataEntity("ext-123"))
        val page = PageImpl(entities, PageRequest.of(0, 20), 1)

        whenever(dataRepository.findByDepartmentContainingIgnoreCase(eq("Engineering"), any<Pageable>()))
            .thenReturn(page)

        // When
        val result = dataService.searchByDepartment("Engineering", 0, 20)

        // Then
        assertNotNull(result)
        assertEquals(1, result.data.size)
        assertEquals(1.0, meterRegistry.counter("data_service_search_by_department").count())
    }

    @Test
    fun `getDataByStatus should return paginated results`() {
        // Given
        val entities = listOf(createDataEntity("ext-123"))
        val page = PageImpl(entities, PageRequest.of(0, 20), 1)

        whenever(dataRepository.findByStatus(eq("active"), any<Pageable>()))
            .thenReturn(page)

        // When
        val result = dataService.getDataByStatus("active", 0, 20)

        // Then
        assertNotNull(result)
        assertEquals(1, result.data.size)
        assertEquals(1.0, meterRegistry.counter("data_service_get_by_status").count())
    }

    @Test
    fun `createOrUpdateData should upsert data and return result`() {
        // Given
        val writeDto = DataWriteDto(
            externalId = "ext-123",
            name = "John Doe",
            email = "john.doe@example.com",
            phone = "+1-555-123-4567",
            department = "Engineering",
            status = "active",
            externalCreatedAt = OffsetDateTime.now().minusDays(10),
            externalUpdatedAt = OffsetDateTime.now().minusDays(1)
        )

        val savedEntity = createDataEntity("ext-123")
        whenever(dataRepository.findByExternalId("ext-123")).thenReturn(Optional.of(savedEntity))

        // When
        val result = dataService.createOrUpdateData(writeDto)

        // Then
        assertNotNull(result)
        assertEquals("ext-123", result.externalId)
        assertEquals("John Doe", result.name)

        verify(dataRepository).upsertByExternalId(
            externalId = "ext-123",
            name = "John Doe",
            email = "john.doe@example.com",
            phone = "+1-555-123-4567",
            department = "Engineering",
            status = "active",
            externalCreatedAt = writeDto.externalCreatedAt,
            externalUpdatedAt = writeDto.externalUpdatedAt,
            createdAt = any(),
            updatedAt = any()
        )

        assertEquals(1.0, meterRegistry.counter("data_service_create_or_update").count())
    }

    @Test
    fun `getDataSummary should return statistics`() {
        // Given
        whenever(dataRepository.count()).thenReturn(100L)
        whenever(dataRepository.countByStatus("active")).thenReturn(80L)
        whenever(dataRepository.countByStatus("inactive")).thenReturn(15L)
        whenever(dataRepository.countByStatus("pending")).thenReturn(5L)
        whenever(dataRepository.findLatestExternalUpdatedAt())
            .thenReturn(OffsetDateTime.now().minusDays(1))

        // When
        val result = dataService.getDataSummary()

        // Then
        assertNotNull(result)
        assertEquals(100L, result["total_count"])
        assertEquals(80L, result["active_count"])
        assertEquals(15L, result["inactive_count"])
        assertEquals(5L, result["pending_count"])
        assertNotNull(result["latest_update_timestamp"])

        assertEquals(1.0, meterRegistry.counter("data_service_get_summary").count())
    }

    @Test
    fun `getDataUpdatedAfter should return filtered results`() {
        // Given
        val timestamp = OffsetDateTime.now().minusDays(1)
        val entities = listOf(createDataEntity("ext-123"))
        whenever(dataRepository.findByExternalUpdatedAtAfter(timestamp)).thenReturn(entities)

        // When
        val result = dataService.getDataUpdatedAfter(timestamp)

        // Then
        assertNotNull(result)
        assertEquals(1, result.size)
        assertEquals("ext-123", result[0].externalId)

        assertEquals(1.0, meterRegistry.counter("data_service_get_updated_after").count())
    }

    private fun createDataEntity(externalId: String): DataEntity {
        return DataEntity(
            id = 1L,
            externalId = externalId,
            name = "John Doe",
            email = "john.doe@example.com",
            phone = "+1-555-123-4567",
            department = "Engineering",
            status = "active",
            externalCreatedAt = OffsetDateTime.now().minusDays(10),
            externalUpdatedAt = OffsetDateTime.now().minusDays(1),
            createdAt = LocalDateTime.now().minusDays(10),
            updatedAt = LocalDateTime.now().minusDays(1)
        )
    }
}