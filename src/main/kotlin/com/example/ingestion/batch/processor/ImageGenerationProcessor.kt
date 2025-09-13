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
import java.time.LocalDateTime
import java.util.Base64
import kotlin.system.measureTimeMillis

@Component
@StepScope
class ImageGenerationProcessor(
    private val webClient: WebClient,
    private val properties: ImageGenerationProperties,
    private val objectMapper: ObjectMapper,
    @Value("#{jobParameters['force'] ?: false}") private val force: Boolean,
    @Value("#{jobParameters['dryRun'] ?: false}") private val dryRun: Boolean
) : ItemProcessor<Place, Place> {

    @Value("\${OLLAMA_BASE_URL:http://localhost:11434}")
    private lateinit var ollamaBaseUrl: String

    @Value("\${OLLAMA_MODEL:llama3}")
    private lateinit var ollamaModel: String

    @Value("\${GEMINI_API_KEY}")
    private lateinit var geminiApiKey: String

    @Value("\${GEMINI_API_URL:https://generativelanguage.googleapis.com/v1beta/models/imagen-3.0-generate-001:generateImage}")
    private lateinit var geminiApiUrl: String

    private val logger = LoggerFactory.getLogger(javaClass)

    override fun process(item: Place): Place? {
        val logPayload = mutableMapOf<String, Any?>("place_id" to item.id)
        val elapsedMs = measureTimeMillis {
            try {
                // 1. Idempotency Check - check gallery column
                if (!force && item.gallery.isNotEmpty()) {
                    // Check if any gallery images exist on disk
                    val hasValidImages = item.gallery.any { imageUrl ->
                        val relativePath = imageUrl.removePrefix("/")
                        val imageFile = File(properties.publish.rootDir, relativePath)
                        imageFile.exists()
                    }
                    if (hasValidImages) {
                        log(logPayload, "skip", "image_exists")
                        return null
                    }
                }

                // 2. Generate image via Gemini (no fallback)
                log(logPayload, "prompt", "started")
                val imagePrompt = generateImagePrompt(item)
                if (imagePrompt == null) {
                    log(logPayload, "skip", "insufficient_input")
                    return null
                }
                log(logPayload, "prompt", "success")

                log(logPayload, "generate", "started")
                val generatedImagePath = generateImage(imagePrompt, item)
                log(logPayload, "generate", "success")

                // Update gallery array with new image
                item.gallery = item.gallery.toMutableList().apply { add(generatedImagePath) }
                log(logPayload, "db_update", "success")

            } catch (e: Exception) {
                log(logPayload, "fail", e.message ?: "Unknown error", e.javaClass.simpleName)
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

        // Use place description and category data as requested
        val categoryInfo = place.tags.joinToString(", ")
        val inputText = "Create a photorealistic image prompt for a ${categoryInfo} place called '${place.name}'. Description: ${place.description}. Generate a detailed image prompt that captures the essence of this place for a public listing thumbnail."

        val response = webClient.post()
            .uri("$ollamaBaseUrl/api/generate")
            .bodyValue(mapOf("model" to ollamaModel, "prompt" to inputText, "stream" to false))
            .retrieve()
            .bodyToMono<OllamaResponse>()
            .block()

        return response?.response?.trim()
    }

    private fun generateImage(prompt: String, place: Place): String {
        val year = LocalDateTime.now().year.toString()
        val month = String.format("%02d", LocalDateTime.now().monthValue)

        val relativeDir = "$year/$month"
        val finalDir = File(properties.publish.rootDir + "/" + properties.publish.basePath + "/" + relativeDir)
        if (!finalDir.exists()) {
            finalDir.mkdirs()
        }

        // Use new naming convention: <placeId>__<sanitizedPlaceName>.jpg (no timestamp needed)
        val sanitizedName = sanitizePlaceName(place.name)
        val filename = "${place.id}__${sanitizedName}.jpg"
        val finalFile = File(finalDir, filename)

        if (dryRun) {
            logger.info("[Dry Run] Would call Gemini API with prompt: $prompt")
            logger.info("[Dry Run] Would save image to: ${finalFile.absolutePath}")
            return "${properties.publish.basePath}/$relativeDir/$filename"
        }

        // Call Gemini API directly (no fallback)
        val imageData = callGeminiApi(prompt)
        
        // Save image data to file
        finalFile.writeBytes(imageData)

        return "${properties.publish.basePath}/$relativeDir/$filename"
    }
    
    /**
     * Sanitize place name for filename usage
     * Replace whitespace/special chars with _, max length ~80 chars
     */
    private fun sanitizePlaceName(placeName: String): String {
        return placeName
            .replace(Regex("[\\s\\W]+"), "_")
            .replace(Regex("_+"), "_")
            .trim('_')
            .take(80)
            .ifEmpty { "unnamed" }
    }

    private fun callGeminiApi(prompt: String): ByteArray {
        var attempts = 0
        var backoff = properties.retry.initialBackoffMs
        
        while (true) {
            try {
                val requestBody = mapOf(
                    "prompt" to mapOf(
                        "text" to prompt
                    ),
                    "generationConfig" to mapOf(
                        "aspectRatio" to "1:1",
                        "negativePrompt" to "blurry, low quality, distorted",
                        "numberOfImages" to 1
                    )
                )

                val response = webClient.post()
                    .uri("$geminiApiUrl?key=$geminiApiKey")
                    .header("Content-Type", "application/json")
                    .bodyValue(requestBody)
                    .retrieve()
                    .bodyToMono<GeminiResponse>()
                    .block()
                    ?: throw RuntimeException("Empty response from Gemini API")

                if (response.candidates.isNullOrEmpty() || response.candidates[0].image?.data.isNullOrBlank()) {
                    throw RuntimeException("No image data received from Gemini API")
                }

                // Decode base64 image data
                val imageData = Base64.getDecoder().decode(response.candidates[0].image!!.data!!)
                return imageData
                
            } catch (e: Exception) {
                val errorMessage = e.message ?: "Unknown error"
                
                // Check for quota/rate limiting errors
                if (errorMessage.contains("429") || errorMessage.contains("quota") || errorMessage.contains("rate limit")) {
                    if (++attempts >= properties.retry.maxAttempts) {
                        throw QuotaExceededException("Gemini API quota exceeded after $attempts attempts: $errorMessage")
                    }
                    logger.warn("Quota error calling Gemini API. Retrying in ${backoff}ms... ($attempts/${properties.retry.maxAttempts})")
                    Thread.sleep(backoff)
                    backoff = (backoff * 2).coerceAtMost(properties.retry.maxBackoffMs)
                } else {
                    // No fallback - if Gemini fails, it's an error as requested
                    throw RuntimeException("Gemini API call failed: $errorMessage", e)
                }
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
    
    data class GeminiResponse(
        val candidates: List<GeminiCandidate>?
    )
    
    data class GeminiCandidate(
        val image: GeminiImage?
    )
    
    data class GeminiImage(
        val data: String?
    )
    
    class QuotaExceededException(message: String) : RuntimeException(message)
}
