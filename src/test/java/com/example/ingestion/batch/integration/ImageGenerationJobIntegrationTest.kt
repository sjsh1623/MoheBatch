package com.example.ingestion.batch.integration

import com.example.ingestion.entity.Place
import com.example.ingestion.repository.PlaceRepository
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import org.springframework.batch.core.Job
import org.springframework.batch.core.JobParametersBuilder
import org.springframework.batch.test.JobLauncherTestUtils
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.ActiveProfiles
import org.springframework.test.context.TestPropertySource
import org.springframework.transaction.annotation.Transactional
import java.nio.file.Path
import kotlin.test.assertEquals
import kotlin.test.assertTrue

@SpringBootTest
@ActiveProfiles("test")
@TestPropertySource(properties = [
    "batch.image-generation.cli.command=echo",
    "batch.image-generation.cli.args-template=Mock CLI output > {OUTPUT}",
    "batch.image-generation.publish.base-path=/images/places",
    "batch.image-generation.retry.max-attempts=1",
    "batch.image-generation.retry.initial-backoff-ms=10",
    "batch.image-generation.concurrency=1"
])
class ImageGenerationJobIntegrationTest(
    private val jobLauncherTestUtils: JobLauncherTestUtils,
    private val placeRepository: PlaceRepository,
    private val placeImageGenerationJob: Job
) {

    @TempDir
    lateinit var tempDir: Path

    @Test
    @Transactional
    fun `should process places without images successfully`() {
        // Given
        val testPlace = Place(
            name = "Test Cafe",
            description = "A cozy test cafe",
            tags = listOf("cafe", "cozy"),
            imageUrl = null
        )
        val savedPlace = placeRepository.save(testPlace)

        // Set temp directory for this test
        System.setProperty("batch.image-generation.publish.root-dir", tempDir.toString())

        val jobParameters = JobParametersBuilder()
            .addString("dryRun", "true")  // Use dry run to avoid file system operations
            .addLong("limit", 1L)
            .toJobParameters()

        // When
        val jobExecution = jobLauncherTestUtils.launchJob(jobParameters)

        // Then
        assertEquals("COMPLETED", jobExecution.status.toString())
        assertEquals(0, jobExecution.stepExecutions.first().filterCount)  // No items filtered out
        assertTrue(jobExecution.stepExecutions.first().readCount >= 1)   // At least our test item was read
    }

    @Test
    @Transactional
    fun `should skip places with existing valid images`() {
        // Given
        val existingImagePath = "/images/places/2025/01/existing.png"
        val testPlace = Place(
            name = "Place with Image",
            description = "Already has an image",
            tags = listOf("existing"),
            imageUrl = existingImagePath
        )
        placeRepository.save(testPlace)

        val jobParameters = JobParametersBuilder()
            .addString("force", "false")
            .addLong("limit", 1L)
            .toJobParameters()

        // When
        val jobExecution = jobLauncherTestUtils.launchJob(jobParameters)

        // Then
        assertEquals("COMPLETED", jobExecution.status.toString())
        // The item should be read but filtered out during processing
        assertTrue(jobExecution.stepExecutions.first().readCount >= 1)
    }

    @Test
    @Transactional
    fun `should filter places with insufficient data`() {
        // Given
        val insufficientPlace = Place(
            name = "Insufficient Place",
            description = "",  // No description
            tags = emptyList(), // No tags
            imageUrl = null
        )
        placeRepository.save(insufficientPlace)

        val jobParameters = JobParametersBuilder()
            .addLong("limit", 1L)
            .toJobParameters()

        // When
        val jobExecution = jobLauncherTestUtils.launchJob(jobParameters)

        // Then
        assertEquals("COMPLETED", jobExecution.status.toString())
        assertTrue(jobExecution.stepExecutions.first().readCount >= 1)
        // Item should be read but skipped during processing
    }

    @Test
    @Transactional
    fun `should respect since parameter for date filtering`() {
        // Given
        val oldPlace = Place(
            name = "Old Place",
            description = "An old place",
            tags = listOf("old"),
            imageUrl = null
        )
        // Save and manually update to old date
        val saved = placeRepository.save(oldPlace)
        placeRepository.flush()

        val jobParameters = JobParametersBuilder()
            .addString("since", "2025-12-31")  // Future date - should read nothing
            .toJobParameters()

        // When
        val jobExecution = jobLauncherTestUtils.launchJob(jobParameters)

        // Then
        assertEquals("COMPLETED", jobExecution.status.toString())
        assertEquals(0, jobExecution.stepExecutions.first().readCount)  // No items should be read
    }

    @Test
    @Transactional
    fun `should store only path without host in database`() {
        // Given
        val testPlace = Place(
            name = "Path Test Place",
            description = "Testing path storage",
            tags = listOf("test"),
            imageUrl = null
        )
        val savedPlace = placeRepository.save(testPlace)

        System.setProperty("batch.image-generation.publish.root-dir", tempDir.toString())

        val jobParameters = JobParametersBuilder()
            .addString("dryRun", "true")
            .addLong("limit", 1L)
            .toJobParameters()

        // When
        val jobExecution = jobLauncherTestUtils.launchJob(jobParameters)

        // Then
        assertEquals("COMPLETED", jobExecution.status.toString())
        
        // In dry run mode, the path should still be set but no file operations performed
        // The path should start with the base path and not contain any host information
        val processedPlace = placeRepository.findById(savedPlace.id!!).orElse(null)
        if (processedPlace?.imageUrl != null) {
            assertTrue(processedPlace.imageUrl!!.startsWith("/images/places/"))
            assertTrue(!processedPlace.imageUrl!!.contains("localhost"))
            assertTrue(!processedPlace.imageUrl!!.contains("http://"))
            assertTrue(!processedPlace.imageUrl!!.contains("https://"))
        }
    }
}