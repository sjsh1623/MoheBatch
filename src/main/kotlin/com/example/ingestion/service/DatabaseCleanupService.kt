package com.example.ingestion.service

import com.example.ingestion.dto.DatabaseCleanupResult
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Service
import org.springframework.web.reactive.function.client.WebClient
import reactor.core.publisher.Mono
import java.time.Duration

@Service
class DatabaseCleanupService(
    private val webClient: WebClient,
    @Value("\${app.mohe-spring.base-url:http://mohe-backend:8080}") private val moheSpringBaseUrl: String
) {

    private val logger = LoggerFactory.getLogger(DatabaseCleanupService::class.java)

    fun cleanupOldAndLowRatedPlaces(): DatabaseCleanupResult {
        logger.info("Starting database cleanup process")
        
        return try {
            // Call MoheSpring cleanup endpoint
            val response = webClient.post()
                .uri("$moheSpringBaseUrl/api/batch/internal/cleanup")
                .retrieve()
                .bodyToMono(DatabaseCleanupResult::class.java)
                .onErrorResume { error ->
                    logger.error("Failed to call cleanup endpoint: ${error.message}")
                    Mono.just(DatabaseCleanupResult(0, emptyList()))
                }
                .block(Duration.ofMinutes(5))
                
            response ?: DatabaseCleanupResult(0, listOf("No response from cleanup service"))
            
        } catch (ex: Exception) {
            logger.error("Database cleanup failed: ${ex.message}", ex)
            DatabaseCleanupResult(0, listOf("Cleanup failed: ${ex.message}"))
        }
    }
}