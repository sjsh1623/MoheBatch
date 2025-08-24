package com.example.ingestion.controller

import io.swagger.v3.oas.annotations.Operation
import io.swagger.v3.oas.annotations.responses.ApiResponse
import io.swagger.v3.oas.annotations.tags.Tag
import org.slf4j.LoggerFactory
import org.springframework.batch.core.Job
import org.springframework.batch.core.JobParameters
import org.springframework.batch.core.JobParametersBuilder
import org.springframework.batch.core.launch.JobLauncher
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.*
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong

@RestController
@RequestMapping("/api/continuous")
@Tag(name = "Continuous Batch Processing", description = "APIs for continuous batch processing with no time limits")
class ContinuousBatchController(
    private val jobLauncher: JobLauncher,
    @Qualifier("regionalPlaceIngestionJob") private val regionalPlaceIngestionJob: Job
) {

    private val logger = LoggerFactory.getLogger(ContinuousBatchController::class.java)
    private val isRunning = AtomicBoolean(false)
    private val batchCounter = AtomicLong(0)
    
    @PostMapping("/start")
    @Operation(
        summary = "Start continuous batch processing",
        description = "Start continuous batch processing - each batch will trigger the next one immediately after completion"
    )
    @ApiResponse(responseCode = "200", description = "Continuous processing started")
    fun startContinuousProcessing(): ResponseEntity<Map<String, Any>> {
        logger.info("POST /api/continuous/start")
        
        if (isRunning.compareAndSet(false, true)) {
            // Start the continuous processing
            executeContinuousBatch()
            
            val response = mapOf(
                "message" to "Continuous batch processing started - NO TIME LIMITS",
                "mode" to "continuous",
                "description" to "Each batch will automatically trigger the next one after completion"
            )
            return ResponseEntity.ok(response)
        } else {
            val response = mapOf(
                "message" to "Continuous batch processing is already running",
                "mode" to "continuous",
                "currentBatch" to batchCounter.get()
            )
            return ResponseEntity.ok(response)
        }
    }
    
    @PostMapping("/stop")
    @Operation(
        summary = "Stop continuous batch processing",
        description = "Stop the continuous batch processing loop"
    )
    @ApiResponse(responseCode = "200", description = "Continuous processing stopped")
    fun stopContinuousProcessing(): ResponseEntity<Map<String, Any>> {
        logger.info("POST /api/continuous/stop")
        
        isRunning.set(false)
        
        val response = mapOf(
            "message" to "Continuous batch processing stopped",
            "mode" to "stopped",
            "totalBatchesExecuted" to batchCounter.get()
        )
        return ResponseEntity.ok(response)
    }
    
    @GetMapping("/status")
    @Operation(
        summary = "Get continuous batch processing status",
        description = "Get the current status of continuous batch processing"
    )
    @ApiResponse(responseCode = "200", description = "Status retrieved")
    fun getContinuousStatus(): ResponseEntity<Map<String, Any>> {
        val status = mapOf(
            "isRunning" to isRunning.get(),
            "totalBatchesExecuted" to batchCounter.get(),
            "mode" to if (isRunning.get()) "continuous" else "stopped",
            "description" to "Continuous batch processing with immediate next-batch triggering"
        )
        return ResponseEntity.ok(status)
    }
    
    private fun executeContinuousBatch() {
        // Execute in background thread
        Thread {
        while (isRunning.get()) {
            try {
                val batchNumber = batchCounter.incrementAndGet()
                logger.info("Executing continuous batch #{} - NO TIME LIMITS", batchNumber)
                
                val jobParameters = JobParametersBuilder()
                    .addLong("timestamp", System.currentTimeMillis())
                    .addString("continuous", "true")
                    .addLong("batchNumber", batchNumber)
                    .toJobParameters()

                val jobExecution = jobLauncher.run(regionalPlaceIngestionJob, jobParameters)
                logger.info("Continuous batch #{} started with execution id: {}", batchNumber, jobExecution.id)
                
                // Wait for the job to complete by polling its status
                while (jobExecution.isRunning) {
                    Thread.sleep(5000) // Check every 5 seconds
                }
                
                val finalStatus = jobExecution.status
                logger.info("Continuous batch #{} completed with status: {}", batchNumber, finalStatus)
                
                if (finalStatus.isUnsuccessful) {
                    logger.warn("Batch #{} failed with status: {}. Waiting 60 seconds before retry...", batchNumber, finalStatus)
                    Thread.sleep(60000) // 60 seconds on failure
                } else {
                    logger.info("Batch #{} completed successfully! Starting next batch immediately...", batchNumber)
                    Thread.sleep(2000) // 2 seconds between successful batches
                }
                
            } catch (ex: Exception) {
                logger.error("Failed to execute continuous batch: ${ex.message}", ex)
                Thread.sleep(30000) // 30 seconds on error
            }
        }
        
        logger.info("Continuous batch processing stopped after {} batches", batchCounter.get())
        }.start()
    }
}