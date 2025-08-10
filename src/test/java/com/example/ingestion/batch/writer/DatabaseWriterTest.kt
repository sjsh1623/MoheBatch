package com.example.ingestion.batch.writer

import com.example.ingestion.entity.DataEntity
import com.example.ingestion.repository.DataRepository
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import org.mockito.Mock
import org.mockito.junit.jupiter.MockitoExtension
import org.mockito.kotlin.*
import org.springframework.batch.item.Chunk
import org.springframework.dao.DataIntegrityViolationException
import java.time.LocalDateTime
import java.time.OffsetDateTime
import java.util.*

@ExtendWith(MockitoExtension::class)
class DatabaseWriterTest {

    @Mock
    private lateinit var dataRepository: DataRepository

    private lateinit var databaseWriter: DatabaseWriter
    private lateinit var meterRegistry: SimpleMeterRegistry

    @BeforeEach
    fun setUp() {
        meterRegistry = SimpleMeterRegistry()
        databaseWriter = DatabaseWriter(dataRepository, meterRegistry)
    }

    @Test
    fun `write should insert new entity when not exists`() {
        // Given
        val entity = createDataEntity("ext-123")
        val chunk = Chunk.of(entity)

        whenever(dataRepository.findByExternalId("ext-123")).thenReturn(Optional.empty())

        // When
        databaseWriter.write(chunk)

        // Then
        verify(dataRepository).findByExternalId("ext-123")
        verify(dataRepository).upsertByExternalId(
            externalId = "ext-123",
            name = "John Doe",
            email = "john.doe@example.com",
            phone = "+1-555-123-4567",
            department = "Engineering",
            status = "active",
            externalCreatedAt = entity.externalCreatedAt,
            externalUpdatedAt = entity.externalUpdatedAt,
            createdAt = any(),
            updatedAt = any()
        )

        assertEquals(1.0, meterRegistry.counter("batch_database_inserts").count())
    }

    @Test
    fun `write should update existing entity when newer`() {
        // Given
        val existingEntity = createDataEntity("ext-123").copy(
            externalUpdatedAt = OffsetDateTime.now().minusDays(2)
        )
        val newEntity = createDataEntity("ext-123").copy(
            externalUpdatedAt = OffsetDateTime.now().minusDays(1)
        )
        val chunk = Chunk.of(newEntity)

        whenever(dataRepository.findByExternalId("ext-123")).thenReturn(Optional.of(existingEntity))

        // When
        databaseWriter.write(chunk)

        // Then
        verify(dataRepository).findByExternalId("ext-123")
        verify(dataRepository).upsertByExternalId(
            externalId = "ext-123",
            name = "John Doe",
            email = "john.doe@example.com",
            phone = "+1-555-123-4567",
            department = "Engineering",
            status = "active",
            externalCreatedAt = newEntity.externalCreatedAt,
            externalUpdatedAt = newEntity.externalUpdatedAt,
            createdAt = existingEntity.createdAt,
            updatedAt = any()
        )

        assertEquals(1.0, meterRegistry.counter("batch_database_updates").count())
    }

    @Test
    fun `write should skip update when existing entity is newer`() {
        // Given
        val existingEntity = createDataEntity("ext-123").copy(
            externalUpdatedAt = OffsetDateTime.now().minusDays(1)
        )
        val olderEntity = createDataEntity("ext-123").copy(
            externalUpdatedAt = OffsetDateTime.now().minusDays(2)
        )
        val chunk = Chunk.of(olderEntity)

        whenever(dataRepository.findByExternalId("ext-123")).thenReturn(Optional.of(existingEntity))

        // When
        databaseWriter.write(chunk)

        // Then
        verify(dataRepository).findByExternalId("ext-123")
        verify(dataRepository, never()).upsertByExternalId(any(), any(), any(), any(), any(), any(), any(), any(), any(), any())

        // Should not increment insert or update counters for skipped items
        assertEquals(0.0, meterRegistry.counter("batch_database_inserts").count())
        assertEquals(0.0, meterRegistry.counter("batch_database_updates").count())
    }

    @Test
    fun `write should handle multiple entities in chunk`() {
        // Given
        val entity1 = createDataEntity("ext-123")
        val entity2 = createDataEntity("ext-456")
        val chunk = Chunk.of(entity1, entity2)

        whenever(dataRepository.findByExternalId("ext-123")).thenReturn(Optional.empty())
        whenever(dataRepository.findByExternalId("ext-456")).thenReturn(Optional.empty())

        // When
        databaseWriter.write(chunk)

        // Then
        verify(dataRepository).findByExternalId("ext-123")
        verify(dataRepository).findByExternalId("ext-456")
        verify(dataRepository, times(2)).upsertByExternalId(any(), any(), any(), any(), any(), any(), any(), any(), any(), any())

        assertEquals(2.0, meterRegistry.counter("batch_database_inserts").count())
    }

    @Test
    fun `write should handle unique constraint violation gracefully`() {
        // Given
        val entity = createDataEntity("ext-123")
        val chunk = Chunk.of(entity)

        whenever(dataRepository.findByExternalId("ext-123")).thenReturn(Optional.empty())
        whenever(dataRepository.upsertByExternalId(any(), any(), any(), any(), any(), any(), any(), any(), any(), any()))
            .thenThrow(DataIntegrityViolationException("unique constraint violation"))

        // When & Then - should not throw exception
        assertDoesNotThrow {
            databaseWriter.write(chunk)
        }

        // Should try upsert twice - once for initial attempt, once for handling violation
        verify(dataRepository, times(2)).upsertByExternalId(any(), any(), any(), any(), any(), any(), any(), any(), any(), any())
        assertEquals(1.0, meterRegistry.counter("batch_database_updates").count())
    }

    @Test
    fun `write should rethrow non-integrity violations`() {
        // Given
        val entity = createDataEntity("ext-123")
        val chunk = Chunk.of(entity)
        val exception = RuntimeException("Database connection error")

        whenever(dataRepository.findByExternalId("ext-123")).thenReturn(Optional.empty())
        whenever(dataRepository.upsertByExternalId(any(), any(), any(), any(), any(), any(), any(), any(), any(), any()))
            .thenThrow(exception)

        // When & Then
        val thrownException = assertThrows<RuntimeException> {
            databaseWriter.write(chunk)
        }

        assertEquals("Database connection error", thrownException.message)
        assertEquals(1.0, meterRegistry.counter("batch_database_write_errors").count())
    }

    @Test
    fun `write should handle empty chunk gracefully`() {
        // Given
        val chunk = Chunk.of<DataEntity>()

        // When & Then - should not throw exception
        assertDoesNotThrow {
            databaseWriter.write(chunk)
        }

        // Should not interact with repository
        verifyNoInteractions(dataRepository)
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