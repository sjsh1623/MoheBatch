package com.example.ingestion.batch.processor

import com.example.ingestion.config.ImageGenerationProperties
import com.example.ingestion.entity.Place
import com.fasterxml.jackson.databind.ObjectMapper
import org.slf4j.LoggerFactory
import org.springframework.batch.core.configuration.annotation.StepScope
import org.springframework.batch.item.ItemProcessor
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Component
import org.springframework.web.reactive.function.client.WebClient
import org.springframework.web.reactive.function.client.bodyToMono
import java.io.File
import java.nio.file.Files
import java.nio.file.StandardCopyOption
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.concurrent.TimeUnit
import kotlin.system.measureTimeMillis

@Component
@StepScope
class ImageGenerationProcessor(
    private val webClient: WebClient,
    private val properties: ImageGenerationProperties,
    private val objectMapper: ObjectMapper,
    @Value("#{jobParameters['force'] ?: false}") private val force: Boolean,
    @Value("#{jobParameters['dryRun'] ?: false}") private val dryRun: Boolean




    @Value("\${OLLAMA_BASE_URL:http://localhost:11434}")
    private lateinit var ollamaBaseUrl: String

    @Value("\${OLLAMA_MODEL:llama3}")
    private lateinit var ollamaModel: String

    private val logger = LoggerFactory.getLogger(javaClass)

    override fun process(item: Place): Place? {
        val logPayload = mutableMapOf<String, Any?>("place_id" to item.id)
        val elapsedMs = measureTimeMillis {
            try {
                // 1. Idempotency Check
                if (!force && item.imageUrl?.isNotBlank() == true) {
                    // Remove leading slash from imageUrl to create proper file path
                    val relativePath = item.imageUrl!!.removePrefix("/")
                    val imageFile = File(properties.publish.rootDir, relativePath)
                    if (imageFile.exists()) {
                        log(logPayload, "skip", "image_exists")
                        return null
                    }
                }

                // 2. Build image prompt via Ollama
                log(logPayload, "prompt", "started")
                val imagePrompt = generateImagePrompt(item)
                if (imagePrompt == null) {
                    log(logPayload, "skip", "insufficient_input")
                    return null
                }
                log(logPayload, "prompt", "success")

                // 3. Generate Image via CLI
                log(logPayload, "generate", "started")
                val imagePath = generateImage(imagePrompt, item.id!!)
                log(logPayload, "generate", "success")

                // 4. Update DB
                log(logPayload, "db_update", "started")
                item.imageUrl = imagePath
                log(logPayload, "db_update", "success")

            } catch (e: Exception) {
                log(logPayload, "fail", e.message, e.javaClass.simpleName)
                // Propagate exception to fail the item
                throw e
            }
        }
        logPayload["elapsed_ms"] = elapsedMs
        logger.info(objectMapper.writeValueAsString(logPayload))

        return item
    }

    private fun generateImagePrompt(place: Place): String? {
        if (place.description.isNullOrBlank() && place.tags.isEmpty()) {
            return null
        }

        val inputText = "Create a photorealistic image prompt for a public listing thumbnail. Focus on a fitting composition (exterior for venues, interior otherwise). Convey a clear subject, natural perspective, and strong mood. Keywords: ${place.name}, ${place.description}, ${place.tags.joinToString(", ")}."

        val response = webClient.post()
            .uri("$ollamaBaseUrl/api/generate")
            .bodyValue(mapOf("model" to ollamaModel, "prompt" to inputText, "stream" to false))
            .retrieve()
            .bodyToMono<OllamaResponse>()
            .block()

        return response?.response?.trim()
    }

    private fun generateImage(prompt: String, placeId: Long): String {
        val timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMddHHmmss"))
        val year = LocalDateTime.now().year.toString()
        val month = String.format("%02d", LocalDateTime.now().monthValue)

        val relativeDir = "$year/$month"
        val finalDir = File(properties.publish.rootDir, "${properties.publish.basePath}/$relativeDir")
        if (!finalDir.exists()) {
            finalDir.mkdirs()
        }

        val filename = "${placeId}_$timestamp.png"
        val tempFile = Files.createTempFile("gemini-", ".png").toFile()
        val finalFile = File(finalDir, filename)

        val command = properties.cli.command + " " + properties.cli.argsTemplate
            .replace("{PROMPT}", prompt.replace("\"", "\\\""))
            .replace("{OUTPUT}", tempFile.absolutePath)

        if (dryRun) {
            logger.info("[Dry Run] Would execute command: $command")
            logger.info("[Dry Run] Would move ${tempFile.absolutePath} to ${finalFile.absolutePath}")
            return "${properties.publish.basePath}/$relativeDir/$filename"
        }

        // Execute command with retry
        executeWithRetry(command)

        Files.move(tempFile.toPath(), finalFile.toPath(), StandardCopyOption.REPLACE_EXISTING)

        return "${properties.publish.basePath}/$relativeDir/$filename"
    }

    private fun executeWithRetry(command: String) {
        var attempts = 0
        var backoff = properties.retry.initialBackoffMs
        while (true) {
            try {
                val process = ProcessBuilder("/bin/sh", "-c", command).start()
                val exited = process.waitFor(120, TimeUnit.SECONDS)
                if (exited && process.exitValue() == 0) {
                    return // Success
                } else {
                    val error = String(process.errorStream.readAllBytes())
                    // 429 is a common "Too Many Requests" status code
                    if (error.contains("429")) {
                        throw QuotaExceededException(error)
                    }
                    throw RuntimeException("CLI execution failed with exit code ${process.exitValue()}: $error")
                }
            } catch (e: QuotaExceededException) {
                if (++attempts >= properties.retry.maxAttempts) throw e
                logger.warn("Quota error for command. Retrying in ${backoff}ms... ($attempts/${properties.retry.maxAttempts})")
                Thread.sleep(backoff)
                backoff = (backoff * 2).coerceAtMost(properties.retry.maxBackoffMs)
            } catch (e: Exception) {
                throw e // Rethrow other exceptions immediately
            }
        }
    }

    private fun log(payload: MutableMap<String, Any?>, stage: String, status: String, error: String? = null, elapsed: Long? = null) {
        payload["stage"] = stage
        payload["status"] = status
        error?.let { payload["error"] = it }
        elapsed?.let { payload["elapsed_ms"] = it }
        logger.info(objectMapper.writeValueAsString(payload))
        // Clear for next stage
        payload.remove("stage"); payload.remove("status"); payload.remove("error"); payload.remove("elapsed_ms");
    }

    data class OllamaResponse(val response: String?)
    class QuotaExceededException(message: String) : RuntimeException(message)
}
