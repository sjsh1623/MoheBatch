package com.example.ingestion.batch.reader

import com.example.ingestion.dto.ExternalApiResponse
import com.example.ingestion.dto.ExternalDataItem
import com.example.ingestion.entity.JobExecutionState
import com.example.ingestion.repository.JobExecutionStateRepository
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Timer
import org.slf4j.LoggerFactory
import org.slf4j.MDC
import org.springframework.batch.item.ItemReader
import org.springframework.beans.factory.annotation.Value
import org.springframework.http.HttpStatusCode
import org.springframework.retry.annotation.Backoff
import org.springframework.retry.annotation.Retryable
import org.springframework.stereotype.Component
import org.springframework.web.reactive.function.client.WebClient
import org.springframework.web.reactive.function.client.WebClientResponseException
import reactor.core.publisher.Mono
import reactor.util.retry.Retry
import java.time.Duration
import java.time.LocalDateTime
import java.time.OffsetDateTime
import java.util.concurrent.atomic.AtomicInteger

@Component
class ExternalApiReader(
    private val webClient: WebClient,
    private val jobExecutionStateRepository: JobExecutionStateRepository,
    private val meterRegistry: MeterRegistry,
    @Value("\${app.external.api.page-size:100}") private val pageSize: Int,
    @Value("\${app.external.api.max-pages:100}") private val maxPages: Int,
    @Value("\${app.batch.job-name:data-ingestion-job}") private val jobName: String
) : ItemReader<ExternalDataItem> {

    private val logger = LoggerFactory.getLogger(ExternalApiReader::class.java)
    private val apiCallTimer = Timer.builder("external_api_calls")
        .description("External API call duration")
        .register(meterRegistry)

    private var currentPage = AtomicInteger(1)
    private var currentBatch = mutableListOf<ExternalDataItem>()
    private var currentIndex = 0
    private var hasMorePages = true
    private var initialized = false

    companion object {
        private const val CORRELATION_ID = "correlationId"
    }

    override fun read(): ExternalDataItem? {
        if (!initialized) {
            initialize()
            initialized = true
        }

        // If we have items in the current batch, return the next one
        if (currentIndex < currentBatch.size) {
            return currentBatch[currentIndex++]
        }

        // If no more pages, we're done
        if (!hasMorePages) {
            logger.info("Completed reading all pages. Total pages processed: ${currentPage.get() - 1}")
            return null
        }

        // Fetch next page
        fetchNextPage()?.let { response ->
            currentBatch = response.data.toMutableList()
            currentIndex = 0
            
            // Update pagination info
            hasMorePages = response.pagination.hasNext && currentPage.get() <= maxPages
            currentPage.incrementAndGet()
            
            // Update job state
            updateJobState()
            
            return if (currentBatch.isNotEmpty()) {
                currentIndex++
                currentBatch[0]
            } else {
                null
            }
        }

        return null
    }

    private fun initialize() {
        val correlationId = generateCorrelationId()
        MDC.put(CORRELATION_ID, correlationId)
        
        logger.info("Initializing ExternalApiReader with page size: $pageSize, max pages: $maxPages")
        
        // Try to resume from last processed state
        val lastState = jobExecutionStateRepository.findByJobName(jobName)
        if (lastState.isPresent) {
            currentPage.set(lastState.get().lastProcessedPage + 1)
            logger.info("Resuming from page ${currentPage.get()} based on last execution state")
        } else {
            logger.info("Starting fresh from page 1")
        }
        
        meterRegistry.gauge("batch_current_page", currentPage)
    }

    @Retryable(
        value = [WebClientResponseException::class],
        maxAttempts = 3,
        backoff = Backoff(delay = 1000, multiplier = 2.0, maxDelay = 10000)
    )
    private fun fetchNextPage(): ExternalApiResponse? {
        val correlationId = MDC.get(CORRELATION_ID) ?: generateCorrelationId()
        
        return apiCallTimer.recordCallable {
            try {
                logger.debug("Fetching page ${currentPage.get()} with page size $pageSize")
                
                val response = webClient.get()
                    .uri { builder ->
                        builder
                            .path("/api/data")
                            .queryParam("page", currentPage.get())
                            .queryParam("size", pageSize)
                            .queryParam("sort", "updated_at,asc")
                            .build()
                    }
                    .header("X-Correlation-ID", correlationId)
                    .retrieve()
                    .onStatus(HttpStatusCode::isError) { response ->
                        logger.error("API returned error status: ${response.statusCode()}")
                        Mono.error(WebClientResponseException(
                            response.statusCode().value(),
                            "API Error",
                            null,
                            null,
                            null
                        ))
                    }
                    .bodyToMono(ExternalApiResponse::class.java)
                    .retryWhen(
                        Retry.backoff(2, Duration.ofSeconds(1))
                            .maxBackoff(Duration.ofSeconds(10))
                            .filter { it is WebClientResponseException && it.statusCode.is5xxServerError }
                            .onRetryExhaustedThrow { _, retrySignal ->
                                logger.error("Exhausted retries for page ${currentPage.get()}")
                                retrySignal.failure()
                            }
                    )
                    .doOnSuccess { result ->
                        logger.info("Successfully fetched page ${currentPage.get()} with ${result.data.size} items")
                        meterRegistry.counter("external_api_requests_success").increment()
                    }
                    .doOnError { error ->
                        logger.error("Failed to fetch page ${currentPage.get()}: ${error.message}", error)
                        meterRegistry.counter("external_api_requests_error").increment()
                    }
                    .block(Duration.ofSeconds(30))

                response
            } catch (ex: Exception) {
                logger.error("Error fetching page ${currentPage.get()}: ${ex.message}", ex)
                meterRegistry.counter("external_api_requests_error").increment()
                throw ex
            }
        }
    }

    private fun updateJobState() {
        try {
            jobExecutionStateRepository.upsertByJobName(
                jobName = jobName,
                lastProcessedPage = currentPage.get() - 1,
                lastProcessedTimestamp = OffsetDateTime.now(),
                totalProcessedRecords = ((currentPage.get() - 1) * pageSize).toLong(),
                lastExecutionStatus = "RUNNING",
                createdAt = LocalDateTime.now(),
                updatedAt = LocalDateTime.now()
            )
            logger.debug("Updated job state for page ${currentPage.get() - 1}")
        } catch (ex: Exception) {
            logger.warn("Failed to update job state: ${ex.message}")
        }
    }

    private fun generateCorrelationId(): String {
        return "batch-${System.currentTimeMillis()}-${Thread.currentThread().id}"
    }
}