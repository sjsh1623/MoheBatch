package com.example.ingestion.dto

import com.fasterxml.jackson.annotation.JsonProperty
import java.time.LocalDateTime

/**
 * Health check response
 */
data class HealthResponse(
    @JsonProperty("status")
    val status: String,
    
    @JsonProperty("timestamp")
    val timestamp: LocalDateTime = LocalDateTime.now(),
    
    @JsonProperty("service")
    val service: String = "MoheBatch",
    
    @JsonProperty("version")
    val version: String = "1.0.0",
    
    @JsonProperty("details")
    val details: Map<String, Any> = emptyMap()
)

/**
 * Readiness check response with database and external API status
 */
data class ReadinessResponse(
    @JsonProperty("status")
    val status: String,
    
    @JsonProperty("timestamp")
    val timestamp: LocalDateTime = LocalDateTime.now(),
    
    @JsonProperty("checks")
    val checks: Map<String, CheckResult>
)

data class CheckResult(
    @JsonProperty("status")
    val status: String,
    
    @JsonProperty("message")
    val message: String? = null,
    
    @JsonProperty("details")
    val details: Map<String, Any> = emptyMap()
)