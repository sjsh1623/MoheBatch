package com.example.ingestion.batch.listener

import com.example.ingestion.repository.JobExecutionStateRepository
import io.micrometer.core.instrument.MeterRegistry
import org.slf4j.LoggerFactory
import org.springframework.batch.core.BatchStatus
import org.springframework.batch.core.JobExecution
import org.springframework.batch.core.JobExecutionListener
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Component
import java.time.Duration
import java.time.LocalDateTime
import java.time.OffsetDateTime

@Component
class JobExecutionListener(
    private val jobExecutionStateRepository: JobExecutionStateRepository,
    private val meterRegistry: MeterRegistry,
    @Value("\${app.batch.job-name:data-ingestion-job}") private val jobName: String
) : JobExecutionListener {

    private val logger = LoggerFactory.getLogger(JobExecutionListener::class.java)

    override fun beforeJob(jobExecution: JobExecution) {
        logger.info("=== Starting job execution ===")
        logger.info("Job Name: {}", jobExecution.jobInstance.jobName)
        logger.info("Job ID: {}", jobExecution.jobId)
        logger.info("Job Parameters: {}", jobExecution.jobParameters.parameters)
        
        // Set up job-level metrics
        meterRegistry.gauge("batch_job_running", 1.0)
        
        // Log job parameters
        val parameters = jobExecution.jobParameters
        parameters.parameters.forEach { (key, value) ->
            logger.info("Job Parameter - {}: {}", key, value.value)
        }
        
        // Initialize or update job state
        try {
            updateJobState("STARTED", jobExecution)
        } catch (ex: Exception) {
            logger.warn("Failed to update job state at start: ${ex.message}")
        }
    }

    override fun afterJob(jobExecution: JobExecution) {
        val duration = if (jobExecution.startTime != null && jobExecution.endTime != null) {
            Duration.between(jobExecution.startTime.toInstant(), jobExecution.endTime.toInstant())
        } else {
            Duration.ZERO
        }

        logger.info("=== Job execution completed ===")
        logger.info("Job Name: {}", jobExecution.jobInstance.jobName)
        logger.info("Job Status: {}", jobExecution.status)
        logger.info("Exit Status: {}", jobExecution.exitStatus.exitCode)
        logger.info("Duration: {} seconds", duration.seconds)
        
        // Log step execution details
        jobExecution.stepExecutions.forEach { stepExecution ->
            logger.info("Step: {} - Status: {}, Read: {}, Write: {}, Skip: {}, Duration: {}ms",
                stepExecution.stepName,
                stepExecution.status,
                stepExecution.readCount,
                stepExecution.writeCount,
                stepExecution.skipCount,
                stepExecution.let { 
                    if (it.startTime != null && it.endTime != null) {
                        Duration.between(it.startTime.toInstant(), it.endTime.toInstant()).toMillis()
                    } else "N/A"
                }
            )
            
            // Update step-level metrics
            meterRegistry.counter("batch_step_read_count", "step", stepExecution.stepName)
                .increment(stepExecution.readCount.toDouble())
            meterRegistry.counter("batch_step_write_count", "step", stepExecution.stepName)
                .increment(stepExecution.writeCount.toDouble())
            meterRegistry.counter("batch_step_skip_count", "step", stepExecution.stepName)
                .increment(stepExecution.skipCount.toDouble())
            
            // Log any failures
            stepExecution.failureExceptions.forEach { exception ->
                logger.error("Step failure in {}: {}", stepExecution.stepName, exception.message, exception)
            }
        }

        // Update job-level metrics
        meterRegistry.gauge("batch_job_running", 0.0)
        meterRegistry.timer("batch_job_duration").record(duration)
        
        when (jobExecution.status) {
            BatchStatus.COMPLETED -> {
                meterRegistry.counter("batch_job_completed").increment()
                logger.info("Job completed successfully")
            }
            BatchStatus.FAILED -> {
                meterRegistry.counter("batch_job_failed").increment()
                logger.error("Job failed")
                
                // Log failure details
                jobExecution.allFailureExceptions.forEach { exception ->
                    logger.error("Job failure: {}", exception.message, exception)
                }
            }
            BatchStatus.STOPPED -> {
                meterRegistry.counter("batch_job_stopped").increment()
                logger.warn("Job was stopped")
            }
            else -> {
                meterRegistry.counter("batch_job_other_status").increment()
                logger.info("Job completed with status: {}", jobExecution.status)
            }
        }

        // Update final job state
        try {
            updateJobState(jobExecution.status.toString(), jobExecution)
        } catch (ex: Exception) {
            logger.warn("Failed to update final job state: ${ex.message}")
        }

        // Log summary metrics
        val totalRead = jobExecution.stepExecutions.sumOf { it.readCount }
        val totalWritten = jobExecution.stepExecutions.sumOf { it.writeCount }
        val totalSkipped = jobExecution.stepExecutions.sumOf { it.skipCount }
        
        logger.info("=== Job Summary ===")
        logger.info("Total Records Read: {}", totalRead)
        logger.info("Total Records Written: {}", totalWritten)
        logger.info("Total Records Skipped: {}", totalSkipped)
        logger.info("Success Rate: {}%", 
            if (totalRead > 0) ((totalWritten.toDouble() / totalRead.toDouble()) * 100).toInt() else 0
        )
    }

    private fun updateJobState(status: String, jobExecution: JobExecution) {
        val totalProcessed = jobExecution.stepExecutions.sumOf { it.writeCount }
        
        jobExecutionStateRepository.upsertByJobName(
            jobName = jobName,
            lastProcessedPage = 0, // Will be updated by reader
            lastProcessedTimestamp = OffsetDateTime.now(),
            totalProcessedRecords = totalProcessed.toLong(),
            lastExecutionStatus = status,
            createdAt = LocalDateTime.now(),
            updatedAt = LocalDateTime.now()
        )
    }
}