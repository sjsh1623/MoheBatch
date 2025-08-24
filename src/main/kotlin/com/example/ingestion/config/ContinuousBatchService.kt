package com.example.ingestion.config

import org.slf4j.LoggerFactory
import org.springframework.batch.core.*
import org.springframework.batch.core.launch.JobLauncher
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.scheduling.annotation.Async
import org.springframework.stereotype.Component
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong
import kotlin.random.Random

@Component
@ConditionalOnProperty(
    value = ["app.batch.continuous.enabled"],
    havingValue = "true",
    matchIfMissing = false
)
class ContinuousBatchService(
    private val jobLauncher: JobLauncher,
    @Qualifier("regionalPlaceIngestionJob") private val regionalPlaceIngestionJob: Job
) : JobExecutionListener {

    private val logger = LoggerFactory.getLogger(ContinuousBatchService::class.java)
    private val isRunning = AtomicBoolean(false)
    private val jobCounter = AtomicLong(0)
    
    fun startContinuousProcessing() {
        if (isRunning.compareAndSet(false, true)) {
            logger.info("Starting continuous batch processing - no time limits")
            executeNextBatch()
        } else {
            logger.info("Continuous batch processing is already running")
        }
    }
    
    fun stopContinuousProcessing() {
        if (isRunning.compareAndSet(true, false)) {
            logger.info("Stopping continuous batch processing")
        }
    }
    
    @Async("batchAsyncExecutor")
    private fun executeNextBatch() {
        if (!isRunning.get()) {
            logger.info("Continuous processing stopped")
            return
        }
        
        try {
            val batchNumber = jobCounter.incrementAndGet()
            logger.info("Executing continuous batch #{}", batchNumber)
            
            val jobParameters = JobParametersBuilder()
                .addLong("timestamp", System.currentTimeMillis())
                .addString("continuous", "true")
                .addLong("batchNumber", batchNumber)
                .addString("forceRestart", "false", false)
                .toJobParameters()

            val jobExecution = jobLauncher.run(regionalPlaceIngestionJob, jobParameters)
            
            // The JobExecutionListener methods will handle what happens after completion
            logger.info("Continuous batch #{} started with execution id: {}", batchNumber, jobExecution.id)
                
        } catch (ex: Exception) {
            logger.error("Failed to execute continuous batch: ${ex.message}", ex)
            // Wait a bit before retrying to avoid rapid failures
            Thread.sleep(30000) // 30 seconds
            if (isRunning.get()) {
                executeNextBatch()
            }
        }
    }
    
    override fun beforeJob(jobExecution: JobExecution) {
        val batchNumber = jobExecution.jobParameters.getLong("batchNumber")
        logger.info("Starting continuous batch #{} - Job Execution ID: {}", batchNumber, jobExecution.id)
    }
    
    override fun afterJob(jobExecution: JobExecution) {
        val batchNumber = jobExecution.jobParameters.getLong("batchNumber")
        val status = jobExecution.status
        val exitStatus = jobExecution.exitStatus
        
        logger.info("Continuous batch #{} completed - Status: {}, Exit Status: {}", batchNumber, status, exitStatus)
        
        when (status) {
            BatchStatus.COMPLETED -> {
                logger.info("Batch #{} completed successfully. Starting next batch immediately...", batchNumber)
                // Small delay to prevent overwhelming the system
                Thread.sleep(2000) // 2 seconds between batches
                executeNextBatch()
            }
            BatchStatus.FAILED -> {
                logger.warn("Batch #{} failed. Waiting 60 seconds before retry...", batchNumber)
                Thread.sleep(60000) // 60 seconds on failure
                if (isRunning.get()) {
                    executeNextBatch()
                }
            }
            else -> {
                logger.info("Batch #{} ended with status: {}. Waiting 30 seconds before next batch...", batchNumber, status)
                Thread.sleep(30000) // 30 seconds for other statuses
                if (isRunning.get()) {
                    executeNextBatch()
                }
            }
        }
    }
    
    fun getStatus(): Map<String, Any> {
        return mapOf(
            "isRunning" to isRunning.get(),
            "totalBatchesExecuted" to jobCounter.get(),
            "mode" to "continuous"
        )
    }
}