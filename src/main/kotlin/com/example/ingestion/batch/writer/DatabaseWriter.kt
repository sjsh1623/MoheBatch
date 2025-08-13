package com.example.ingestion.batch.writer

import com.example.ingestion.dto.ProcessedPlace
import com.example.ingestion.entity.DataEntity
import io.micrometer.core.instrument.MeterRegistry
import org.slf4j.LoggerFactory
import org.springframework.batch.item.Chunk
import org.springframework.batch.item.ItemWriter
import org.springframework.beans.factory.annotation.Value
import org.springframework.http.HttpHeaders
import org.springframework.http.MediaType
import org.springframework.stereotype.Component
import org.springframework.web.reactive.function.client.WebClient
import org.springframework.web.reactive.function.client.WebClientResponseException
import reactor.core.publisher.Mono
import reactor.util.retry.Retry
import java.time.Duration

@Component
class MoheSpringApiWriter(
    private val webClient: WebClient,
    private val meterRegistry: MeterRegistry,
    @Value("\${app.mohe-spring.base-url:http://mohe-backend:8080}") private val moheSpringBaseUrl: String
) : ItemWriter<ProcessedPlace> {

    private val logger = LoggerFactory.getLogger(MoheSpringApiWriter::class.java)

    override fun write(chunk: Chunk<out ProcessedPlace>) {
        if (chunk.isEmpty()) {
            return
        }

        logger.info("Sending chunk of ${chunk.size()} items to MoheSpring API")
        
        try {
            // Convert ProcessedPlace to MoheSpring API format
            val apiRequests = chunk.items.map { processedPlace ->
                mapToApiRequest(processedPlace)
            }
            
            // Send to MoheSpring internal ingestion API
            val response = sendToMoheSpringApi(apiRequests)
            
            // Update metrics based on response
            meterRegistry.counter("batch_api_requests_success").increment()
            meterRegistry.counter("batch_database_inserts").increment(response.insertedCount.toDouble())
            meterRegistry.counter("batch_database_updates").increment(response.updatedCount.toDouble())
            meterRegistry.counter("batch_items_skipped").increment(response.skippedCount.toDouble())
            meterRegistry.counter("batch_keywords_generated").increment(response.keywordGeneratedCount.toDouble())
            
            if (response.errorCount > 0) {
                logger.warn("MoheSpring API reported ${response.errorCount} errors: ${response.errors}")
                meterRegistry.counter("batch_api_write_errors").increment(response.errorCount.toDouble())
            }
            
            logger.info("Successfully sent chunk to MoheSpring API: ${response.insertedCount} inserted, ${response.updatedCount} updated, ${response.skippedCount} skipped, ${response.errorCount} errors")
            
        } catch (ex: Exception) {
            logger.error("Failed to send chunk to MoheSpring API: ${ex.message}", ex)
            meterRegistry.counter("batch_api_requests_error").increment()
            
            // Re-throw for chunk-level error handling
            throw ex
        }
    }

    private fun mapToApiRequest(processedPlace: ProcessedPlace): InternalPlaceIngestRequest {
        return InternalPlaceIngestRequest(
            naverPlaceId = processedPlace.naverPlaceId,
            googlePlaceId = processedPlace.googlePlaceId,
            name = processedPlace.name,
            description = processedPlace.description,
            category = processedPlace.category,
            address = processedPlace.address,
            roadAddress = processedPlace.roadAddress,
            latitude = processedPlace.latitude,
            longitude = processedPlace.longitude,
            phone = processedPlace.phone,
            websiteUrl = processedPlace.websiteUrl,
            rating = processedPlace.rating,
            userRatingsTotal = processedPlace.userRatingsTotal,
            priceLevel = processedPlace.priceLevel,
            types = processedPlace.types,
            openingHours = processedPlace.openingHours,
            imageUrl = processedPlace.imageUrl,
            sourceFlags = processedPlace.sourceFlags,
            naverRawData = processedPlace.naverRawData,
            googleRawData = processedPlace.googleRawData
        )
    }

    private fun sendToMoheSpringApi(requests: List<InternalPlaceIngestRequest>): InternalPlaceIngestResponse {
        return webClient.post()
            .uri("$moheSpringBaseUrl/api/batch/internal/ingest/place")
            .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
            .bodyValue(requests)
            .retrieve()
            .onStatus({ status -> status.isError }) { response ->
                logger.error("MoheSpring API returned error status: ${response.statusCode()}")
                Mono.error(WebClientResponseException(
                    response.statusCode().value(),
                    "MoheSpring API Error",
                    null,
                    null,
                    null
                ))
            }
            .bodyToMono(InternalApiResponse::class.java)
            .retryWhen(
                Retry.backoff(3, Duration.ofSeconds(1))
                    .maxBackoff(Duration.ofSeconds(10))
                    .filter { it is WebClientResponseException && it.statusCode.is5xxServerError }
                    .onRetryExhaustedThrow { _, retrySignal ->
                        logger.error("Exhausted retries for MoheSpring API call")
                        retrySignal.failure()
                    }
            )
            .doOnSuccess { result ->
                logger.debug("Successfully sent ${requests.size} places to MoheSpring internal ingestion API")
            }
            .doOnError { error ->
                logger.error("Failed to send places to MoheSpring API: ${error.message}", error)
            }
            .block(Duration.ofSeconds(120)) // Longer timeout for Ollama processing
            ?.let { apiResponse ->
                // Extract the actual response data
                if (apiResponse.success) {
                    apiResponse.data
                } else {
                    throw RuntimeException("MoheSpring API returned error: ${apiResponse.message}")
                }
            } ?: throw RuntimeException("No response received from MoheSpring API")
    }
}

// DTOs for MoheSpring internal API communication
data class InternalPlaceIngestRequest(
    val naverPlaceId: String,
    val googlePlaceId: String?,
    val name: String,
    val description: String,
    val category: String,
    val address: String,
    val roadAddress: String?,
    val latitude: java.math.BigDecimal,
    val longitude: java.math.BigDecimal,
    val phone: String?,
    val websiteUrl: String?,
    val rating: Double?,
    val userRatingsTotal: Int?,
    val priceLevel: Int?,
    val types: List<String>,
    val openingHours: String?, // JSON string
    val imageUrl: String?,
    val sourceFlags: Map<String, Any>,
    val naverRawData: String, // JSON string
    val googleRawData: String? // JSON string
)

data class InternalApiResponse(
    val success: Boolean,
    val data: InternalPlaceIngestResponse,
    val message: String? = null
)

data class InternalPlaceIngestResponse(
    val processedCount: Int,
    val insertedCount: Int,
    val updatedCount: Int,
    val skippedCount: Int,
    val errorCount: Int,
    val keywordGeneratedCount: Int,
    val errors: List<String>
)