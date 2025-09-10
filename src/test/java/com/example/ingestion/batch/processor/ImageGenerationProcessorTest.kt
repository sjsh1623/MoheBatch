package com.example.ingestion.batch.processor

import com.example.ingestion.config.ImageGenerationProperties
import com.example.ingestion.entity.Place
import com.fasterxml.jackson.databind.ObjectMapper
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import org.mockito.Mockito.*
import org.springframework.web.reactive.function.client.WebClient
import reactor.core.publisher.Mono
import java.io.File
import java.nio.file.Path
import kotlin.test.assertEquals
import kotlin.test.assertNull
import kotlin.test.assertTrue

class ImageGenerationProcessorTest {

    @TempDir
    lateinit var tempDir: Path

    private lateinit var webClient: WebClient
    private lateinit var properties: ImageGenerationProperties
    private lateinit var objectMapper: ObjectMapper
    private lateinit var processor: ImageGenerationProcessor

    @BeforeEach
    fun setUp() {
        webClient = mock(WebClient::class.java)
        objectMapper = ObjectMapper()
        
        properties = ImageGenerationProperties(
            cli = ImageGenerationProperties.CliProperties(
                command = "echo",
                argsTemplate = "Generated image for: {PROMPT} > {OUTPUT}",
                imageSize = "1024x1024"
            ),
            publish = ImageGenerationProperties.PublishProperties(
                rootDir = tempDir.toString(),
                basePath = "/images/places"
            ),
            retry = ImageGenerationProperties.RetryProperties(
                maxAttempts = 3,
                initialBackoffMs = 100,
                maxBackoffMs = 1000
            ),
            concurrency = 2
        )
        
        processor = ImageGenerationProcessor(webClient, properties, objectMapper, false, false)
    }

    @Test
    fun `should skip place with existing image when force is false`() {
        // Given
        val existingImageFile = File(tempDir.toFile(), "existing.png")
        existingImageFile.createNewFile()
        
        val place = Place(
            id = 1L,
            name = "Test Place",
            description = "A test place",
            tags = listOf("tag1", "tag2"),
            imageUrl = existingImageFile.absolutePath
        )
        
        // When
        val result = processor.process(place)
        
        // Then
        assertNull(result)
    }

    @Test
    fun `should skip place with insufficient input data`() {
        // Given
        val place = Place(
            id = 1L,
            name = "Test Place",
            description = "",
            tags = emptyList(),
            imageUrl = null
        )
        
        // When
        val result = processor.process(place)
        
        // Then
        assertNull(result)
    }

    @Test
    fun `should generate path-only URL without host`() {
        // Given
        val place = Place(
            id = 123L,
            name = "Test Place",
            description = "A wonderful test place",
            tags = listOf("cafe", "cozy"),
            imageUrl = null
        )
        
        // Mock WebClient chain
        val requestBodyUriSpec = mock(WebClient.RequestBodyUriSpec::class.java)
        val requestBodySpec = mock(WebClient.RequestBodySpec::class.java)
        val responseSpec = mock(WebClient.ResponseSpec::class.java)
        
        `when`(webClient.post()).thenReturn(requestBodyUriSpec)
        `when`(requestBodyUriSpec.uri(anyString())).thenReturn(requestBodySpec)
        `when`(requestBodySpec.bodyValue(any())).thenReturn(requestBodySpec)
        `when`(requestBodySpec.retrieve()).thenReturn(responseSpec)
        `when`(responseSpec.bodyToMono(any<Class<*>>())).thenReturn(
            Mono.just(ImageGenerationProcessor.OllamaResponse("A beautiful cafe with cozy atmosphere"))
        )

        // When - using dry run to avoid actual file operations
        val dryRunProcessor = ImageGenerationProcessor(webClient, properties, objectMapper, false, true)
        val result = dryRunProcessor.process(place)
        
        // Then
        assertTrue(result != null)
        assertTrue(result.imageUrl!!.startsWith("/images/places/"))
        assertTrue(result.imageUrl!!.contains("123_"))
        assertTrue(result.imageUrl!!.endsWith(".png"))
        // Ensure no host is included
        assertTrue(!result.imageUrl!!.contains("localhost"))
        assertTrue(!result.imageUrl!!.contains("http://"))
    }

    @Test
    fun `should handle quota exhaustion with retry`() {
        // Given
        val place = Place(
            id = 1L,
            name = "Test Place",
            description = "A test place",
            tags = listOf("tag1"),
            imageUrl = null
        )
        
        // Mock Ollama response
        val requestBodyUriSpec = mock(WebClient.RequestBodyUriSpec::class.java)
        val requestBodySpec = mock(WebClient.RequestBodySpec::class.java)
        val responseSpec = mock(WebClient.ResponseSpec::class.java)
        
        `when`(webClient.post()).thenReturn(requestBodyUriSpec)
        `when`(requestBodyUriSpec.uri(anyString())).thenReturn(requestBodySpec)
        `when`(requestBodySpec.bodyValue(any())).thenReturn(requestBodySpec)
        `when`(requestBodySpec.retrieve()).thenReturn(responseSpec)
        `when`(responseSpec.bodyToMono(any<Class<*>>())).thenReturn(
            Mono.just(ImageGenerationProcessor.OllamaResponse("Test prompt"))
        )

        // Use a processor with CLI command that simulates quota error
        val quotaProperties = properties.copy(
            cli = properties.cli.copy(
                command = "sh",
                argsTemplate = "-c 'echo \"429: Too Many Requests\" >&2; exit 1'"
            ),
            retry = ImageGenerationProperties.RetryProperties(
                maxAttempts = 1,  // Fail quickly for test
                initialBackoffMs = 10,
                maxBackoffMs = 50
            )
        )
        
        val quotaProcessor = ImageGenerationProcessor(webClient, quotaProperties, objectMapper, false, false)
        
        // When/Then - should throw QuotaExceededException
        try {
            quotaProcessor.process(place)
            assert(false) { "Expected QuotaExceededException" }
        } catch (e: Exception) {
            assertTrue(e.message?.contains("429") == true || e.cause?.message?.contains("429") == true)
        }
    }

    @Test
    fun `dry run should not create files or update database`() {
        // Given
        val place = Place(
            id = 1L,
            name = "Test Place", 
            description = "A test place",
            tags = listOf("tag1"),
            imageUrl = null
        )
        
        // Mock Ollama response
        val requestBodyUriSpec = mock(WebClient.RequestBodyUriSpec::class.java)
        val requestBodySpec = mock(WebClient.RequestBodySpec::class.java)
        val responseSpec = mock(WebClient.ResponseSpec::class.java)
        
        `when`(webClient.post()).thenReturn(requestBodyUriSpec)
        `when`(requestBodyUriSpec.uri(anyString())).thenReturn(requestBodySpec)
        `when`(requestBodySpec.bodyValue(any())).thenReturn(requestBodySpec)
        `when`(requestBodySpec.retrieve()).thenReturn(responseSpec)
        `when`(responseSpec.bodyToMono(any<Class<*>>())).thenReturn(
            Mono.just(ImageGenerationProcessor.OllamaResponse("Test prompt"))
        )

        val dryRunProcessor = ImageGenerationProcessor(webClient, properties, objectMapper, false, true)
        
        // When
        val result = dryRunProcessor.process(place)
        
        // Then
        assertTrue(result != null)
        assertTrue(result.imageUrl!!.startsWith("/images/places/"))
        
        // Verify no actual files were created
        val expectedDir = File(tempDir.toFile(), "images/places")
        assertTrue(!expectedDir.exists() || expectedDir.listFiles()?.isEmpty() == true)
    }
}