package com.example.ingestion.config

import org.slf4j.LoggerFactory
import org.springframework.batch.core.Job
import org.springframework.batch.core.JobParameters
import org.springframework.batch.core.JobParametersBuilder
import org.springframework.batch.core.launch.JobLauncher
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component

// DISABLED - OLD BROKEN SCHEDULER
/* @Component
@ConditionalOnProperty(
    value = ["app.batch.scheduling.old-job-enabled"],
    havingValue = "true",
    matchIfMissing = false  // DISABLED - this job is broken
)
class SchedulingConfiguration(
    private val jobLauncher: JobLauncher,
    private val regionalPlaceIngestionJob: Job
) {

    private val logger = LoggerFactory.getLogger(SchedulingConfiguration::class.java)

    @Scheduled(cron = "\${app.batch.scheduling.cron:0 0 2 * * ?}")  // Daily at 2 AM by default
    fun runScheduledDataIngestion() {
        try {
            logger.info("Starting scheduled data ingestion job")
            
            val jobParameters: JobParameters = JobParametersBuilder()
                .addLong("timestamp", System.currentTimeMillis())
                .addString("scheduled", "true")
                .toJobParameters()

            val jobExecution = jobLauncher.run(regionalPlaceIngestionJob, jobParameters)
            
            logger.info("Scheduled job completed with status: {}, execution id: {}", 
                jobExecution.status, jobExecution.id)
                
        } catch (ex: Exception) {
            logger.error("Failed to run scheduled data ingestion job: ${ex.message}", ex)
        }
    }
} */