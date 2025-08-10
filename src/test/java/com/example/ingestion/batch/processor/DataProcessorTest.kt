package com.example.ingestion.batch.processor

import com.example.ingestion.dto.ExternalDataItem
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.time.OffsetDateTime

class DataProcessorTest {

    private lateinit var dataProcessor: DataProcessor
    private lateinit var meterRegistry: SimpleMeterRegistry

    @BeforeEach
    fun setUp() {
        meterRegistry = SimpleMeterRegistry()
        dataProcessor = DataProcessor(meterRegistry)
    }

    @Test
    fun `process should return valid entity for valid input`() {
        // Given
        val item = ExternalDataItem(
            id = "ext-123",
            name = "John Doe",
            email = "john.doe@example.com",
            phone = "+1-555-123-4567",
            department = "Engineering",
            status = "active",
            createdAt = OffsetDateTime.now().minusDays(10),
            updatedAt = OffsetDateTime.now().minusDays(1)
        )

        // When
        val result = dataProcessor.process(item)

        // Then
        assertNotNull(result)
        assertEquals("ext-123", result!!.externalId)
        assertEquals("John Doe", result.name)
        assertEquals("john.doe@example.com", result.email)
        assertEquals("+1-555-123-4567", result.phone)
        assertEquals("Engineering", result.department)
        assertEquals("active", result.status)
        
        // Verify metrics
        assertEquals(1.0, meterRegistry.counter("batch_items_processed").count())
    }

    @Test
    fun `process should normalize email to lowercase`() {
        // Given
        val item = ExternalDataItem(
            id = "ext-123",
            name = "John Doe",
            email = "JOHN.DOE@EXAMPLE.COM",
            phone = null,
            department = null,
            status = "active",
            createdAt = OffsetDateTime.now().minusDays(10),
            updatedAt = OffsetDateTime.now().minusDays(1)
        )

        // When
        val result = dataProcessor.process(item)

        // Then
        assertNotNull(result)
        assertEquals("john.doe@example.com", result!!.email)
    }

    @Test
    fun `process should return null for invalid email format`() {
        // Given
        val item = ExternalDataItem(
            id = "ext-123",
            name = "John Doe",
            email = "invalid-email",
            phone = null,
            department = null,
            status = "active",
            createdAt = OffsetDateTime.now().minusDays(10),
            updatedAt = OffsetDateTime.now().minusDays(1)
        )

        // When
        val result = dataProcessor.process(item)

        // Then
        assertNull(result)
        assertEquals(1.0, meterRegistry.counter("batch_items_skipped").count())
    }

    @Test
    fun `process should return null for blank external ID`() {
        // Given
        val item = ExternalDataItem(
            id = "",
            name = "John Doe",
            email = "john.doe@example.com",
            phone = null,
            department = null,
            status = "active",
            createdAt = OffsetDateTime.now().minusDays(10),
            updatedAt = OffsetDateTime.now().minusDays(1)
        )

        // When
        val result = dataProcessor.process(item)

        // Then
        assertNull(result)
        assertEquals(1.0, meterRegistry.counter("batch_items_skipped").count())
    }

    @Test
    fun `process should return null for blank name`() {
        // Given
        val item = ExternalDataItem(
            id = "ext-123",
            name = "   ",
            email = "john.doe@example.com",
            phone = null,
            department = null,
            status = "active",
            createdAt = OffsetDateTime.now().minusDays(10),
            updatedAt = OffsetDateTime.now().minusDays(1)
        )

        // When
        val result = dataProcessor.process(item)

        // Then
        assertNull(result)
        assertEquals(1.0, meterRegistry.counter("batch_items_skipped").count())
    }

    @Test
    fun `process should return null for null email`() {
        // Given
        val item = ExternalDataItem(
            id = "ext-123",
            name = "John Doe",
            email = null,
            phone = null,
            department = null,
            status = "active",
            createdAt = OffsetDateTime.now().minusDays(10),
            updatedAt = OffsetDateTime.now().minusDays(1)
        )

        // When
        val result = dataProcessor.process(item)

        // Then
        assertNull(result)
        assertEquals(1.0, meterRegistry.counter("batch_items_skipped").count())
    }

    @Test
    fun `process should return null for missing timestamps`() {
        // Given
        val item = ExternalDataItem(
            id = "ext-123",
            name = "John Doe",
            email = "john.doe@example.com",
            phone = null,
            department = null,
            status = "active",
            createdAt = null,
            updatedAt = OffsetDateTime.now().minusDays(1)
        )

        // When
        val result = dataProcessor.process(item)

        // Then
        assertNull(result)
        assertEquals(1.0, meterRegistry.counter("batch_items_skipped").count())
    }

    @Test
    fun `process should default status to active for invalid status`() {
        // Given
        val item = ExternalDataItem(
            id = "ext-123",
            name = "John Doe",
            email = "john.doe@example.com",
            phone = null,
            department = null,
            status = "invalid-status",
            createdAt = OffsetDateTime.now().minusDays(10),
            updatedAt = OffsetDateTime.now().minusDays(1)
        )

        // When
        val result = dataProcessor.process(item)

        // Then
        assertNotNull(result)
        assertEquals("active", result!!.status)
        assertEquals(1.0, meterRegistry.counter("batch_items_processed").count())
    }

    @Test
    fun `process should normalize status to lowercase`() {
        // Given
        val item = ExternalDataItem(
            id = "ext-123",
            name = "John Doe",
            email = "john.doe@example.com",
            phone = null,
            department = null,
            status = "INACTIVE",
            createdAt = OffsetDateTime.now().minusDays(10),
            updatedAt = OffsetDateTime.now().minusDays(1)
        )

        // When
        val result = dataProcessor.process(item)

        // Then
        assertNotNull(result)
        assertEquals("inactive", result!!.status)
        assertEquals(1.0, meterRegistry.counter("batch_items_processed").count())
    }

    @Test
    fun `process should trim whitespace from fields`() {
        // Given
        val item = ExternalDataItem(
            id = "ext-123",
            name = "  John Doe  ",
            email = "  john.doe@example.com  ",
            phone = "  +1-555-123-4567  ",
            department = "  Engineering  ",
            status = "active",
            createdAt = OffsetDateTime.now().minusDays(10),
            updatedAt = OffsetDateTime.now().minusDays(1)
        )

        // When
        val result = dataProcessor.process(item)

        // Then
        assertNotNull(result)
        assertEquals("John Doe", result!!.name)
        assertEquals("john.doe@example.com", result.email)
        assertEquals("+1-555-123-4567", result.phone)
        assertEquals("Engineering", result.department)
    }

    @Test
    fun `process should handle null optional fields`() {
        // Given
        val item = ExternalDataItem(
            id = "ext-123",
            name = "John Doe",
            email = "john.doe@example.com",
            phone = null,
            department = null,
            status = null,
            createdAt = OffsetDateTime.now().minusDays(10),
            updatedAt = OffsetDateTime.now().minusDays(1)
        )

        // When
        val result = dataProcessor.process(item)

        // Then
        assertNotNull(result)
        assertEquals("ext-123", result!!.externalId)
        assertEquals("John Doe", result.name)
        assertEquals("john.doe@example.com", result.email)
        assertNull(result.phone)
        assertNull(result.department)
        assertEquals("active", result.status) // Should default to active
    }
}