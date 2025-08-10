package com.example.ingestion.controller

import com.example.ingestion.dto.CheckResult
import com.example.ingestion.dto.HealthResponse
import com.example.ingestion.dto.ReadinessResponse
import com.example.ingestion.repository.DataRepository
import io.swagger.v3.oas.annotations.Operation
import io.swagger.v3.oas.annotations.responses.ApiResponse
import io.swagger.v3.oas.annotations.tags.Tag
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController
import org.springframework.web.reactive.function.client.WebClient
import java.time.Duration
import java.time.LocalDateTime

@RestController
@RequestMapping("/health")
@Tag(name = "Health Check", description = "Health and readiness check endpoints")
class HealthController(
    private val dataRepository: DataRepository,
    private val webClient: WebClient,
    @Value("\${app.external.api.base-url}") private val externalApiBaseUrl: String
) {

    private val logger = LoggerFactory.getLogger(HealthController::class.java)

    @GetMapping
    @Operation(
        summary = "Health check",
        description = "Basic health check endpoint"
    )
    @ApiResponse(responseCode = "200", description = "Service is healthy")
    fun health(): ResponseEntity<HealthResponse> {
        logger.debug("Health check requested")
        
        val response = HealthResponse(
            status = "UP",
            details = mapOf(
                "uptime" to System.currentTimeMillis(),
                "memory" to mapOf(
                    "free" to Runtime.getRuntime().freeMemory(),
                    "total" to Runtime.getRuntime().totalMemory(),
                    "max" to Runtime.getRuntime().maxMemory()
                )
            )
        )
        
        return ResponseEntity.ok(response)
    }

    @GetMapping("/ready")
    @Operation(
        summary = "Readiness check",
        description = "Readiness check with database and external API connectivity"
    )
    @ApiResponse(responseCode = "200", description = "Service is ready")
    @ApiResponse(responseCode = "503", description = "Service is not ready")
    fun readiness(): ResponseEntity<ReadinessResponse> {
        logger.debug("Readiness check requested")
        
        val checks = mutableMapOf<String, CheckResult>()
        var overallStatus = "UP"

        // Database connectivity check
        try {
            val count = dataRepository.count()
            checks["database"] = CheckResult(
                status = "UP",
                message = "Database connection successful",
                details = mapOf("total_records" to count)
            )
            logger.debug("Database check passed - total records: {}", count)
        } catch (ex: Exception) {
            checks["database"] = CheckResult(
                status = "DOWN",
                message = "Database connection failed: ${ex.message}"
            )
            overallStatus = "DOWN"
            logger.error("Database check failed: ${ex.message}")
        }

        // External API connectivity check
        try {
            val response = webClient.get()
                .uri("/health")
                .retrieve()
                .toBodilessEntity()
                .timeout(Duration.ofSeconds(5))
                .block()

            if (response?.statusCode?.is2xxSuccessful == true) {
                checks["external_api"] = CheckResult(
                    status = "UP",
                    message = "External API is reachable",
                    details = mapOf("base_url" to externalApiBaseUrl)
                )
                logger.debug("External API check passed")
            } else {
                checks["external_api"] = CheckResult(
                    status = "DOWN",
                    message = "External API returned non-2xx status: ${response?.statusCode}"
                )
                overallStatus = "DOWN"
                logger.warn("External API check failed - status: {}", response?.statusCode)
            }
        } catch (ex: Exception) {
            checks["external_api"] = CheckResult(
                status = "DOWN",
                message = "External API unreachable: ${ex.message}"
            )
            // Don't fail overall status for external API issues - it's not critical for serving data
            logger.warn("External API check failed: ${ex.message}")
        }

        val readinessResponse = ReadinessResponse(
            status = overallStatus,
            checks = checks
        )

        val httpStatus = if (overallStatus == "UP") {
            ResponseEntity.ok(readinessResponse)
        } else {
            ResponseEntity.status(503).body(readinessResponse)
        }

        return httpStatus
    }

    @GetMapping("/liveness")
    @Operation(
        summary = "Liveness check",
        description = "Basic liveness check - indicates if the service should be restarted"
    )
    @ApiResponse(responseCode = "200", description = "Service is alive")
    fun liveness(): ResponseEntity<HealthResponse> {
        logger.debug("Liveness check requested")
        
        // Simple liveness check - just verify the service is responding
        val response = HealthResponse(
            status = "UP",
            timestamp = LocalDateTime.now(),
            details = mapOf(
                "check_type" to "liveness",
                "threads" to mapOf(
                    "active" to Thread.activeCount(),
                    "peak" to Thread.getAllStackTraces().size
                )
            )
        )
        
        return ResponseEntity.ok(response)
    }
}