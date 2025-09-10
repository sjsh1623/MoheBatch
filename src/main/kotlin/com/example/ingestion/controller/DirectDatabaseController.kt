package com.example.ingestion.controller

import org.springframework.batch.core.Job
import org.springframework.batch.core.JobParametersBuilder
import org.springframework.batch.core.launch.JobLauncher
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.web.bind.annotation.*
import org.slf4j.LoggerFactory

/**
 * Direct Database Controller - API Bypass Trigger
 * 
 * Provides REST endpoints to trigger the direct database insertion job
 * that bypasses all external APIs and directly inserts place data.
 * 
 * Use this when you need immediate continuous data flow without
 * depending on external API credentials or availability.
 */
@RestController
@RequestMapping("/api/direct")
class DirectDatabaseController(
    private val jobLauncher: JobLauncher,
    @Qualifier("directDatabasePlaceIngestionJob") 
    private val directDatabaseJob: Job
) {
    
    private val logger = LoggerFactory.getLogger(DirectDatabaseController::class.java)

    @PostMapping("/start-continuous-insertion")
    fun startContinuousInsertion(): Map<String, Any> {
        logger.info("🚀 STARTING DIRECT DATABASE CONTINUOUS INSERTION - NO APIS NEEDED")
        
        return try {
            val jobParameters = JobParametersBuilder()
                .addLong("timestamp", System.currentTimeMillis())
                .addString("mode", "continuous")
                .addString("bypass", "all_external_apis")
                .toJobParameters()
            
            val jobExecution = jobLauncher.run(directDatabaseJob, jobParameters)
            
            logger.info("✅ Direct database job started successfully")
            logger.info("📊 Job ID: {}, Status: {}", jobExecution.id, jobExecution.status)
            
            mapOf(
                "success" to true,
                "message" to "Direct database continuous insertion started successfully",
                "jobId" to jobExecution.id,
                "status" to jobExecution.status.toString(),
                "mode" to "BYPASS_ALL_APIS",
                "description" to "Places will be inserted directly into database without any external API calls",
                "timestamp" to System.currentTimeMillis()
            )
            
        } catch (e: Exception) {
            logger.error("❌ Failed to start direct database job: {}", e.message, e)
            
            mapOf(
                "success" to false,
                "error" to (e.message ?: "Unknown error"),
                "timestamp" to System.currentTimeMillis()
            )
        }
    }

    @GetMapping("/status")
    fun getStatus(): Map<String, Any> {
        return mapOf(
            "service" to "Direct Database Place Insertion",
            "description" to "Bypasses all external APIs and generates realistic Korean place data directly",
            "features" to listOf(
                "No external API dependencies",
                "Realistic Korean place names and locations", 
                "Seoul coordinate generation",
                "Mock Naver/Google/Ollama data creation",
                "Continuous data insertion"
            ),
            "usage" to "POST /api/direct/start-continuous-insertion to begin data flow",
            "timestamp" to System.currentTimeMillis()
        )
    }
}