package com.example.ingestion.config

import com.example.ingestion.service.DatabaseCleanupService
import org.slf4j.LoggerFactory
import org.springframework.batch.core.Job
import org.springframework.batch.core.JobParameters
import org.springframework.batch.core.JobParametersBuilder
import org.springframework.batch.core.launch.JobLauncher
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component

@Component
@ConditionalOnProperty(
    value = ["app.batch.scheduling.enabled"],
    havingValue = "true",
    matchIfMissing = false
)
class RegionalSchedulingConfiguration(
    private val jobLauncher: JobLauncher,
    @Qualifier("regionalPlaceIngestionJob") private val regionalPlaceIngestionJob: Job,
    private val databaseCleanupService: DatabaseCleanupService
) {

    private val logger = LoggerFactory.getLogger(RegionalSchedulingConfiguration::class.java)

    @Scheduled(cron = "\${app.batch.scheduling.cron:*/10 * * * * ?}")  // Every 10 seconds for continuous processing
    fun runRegionalPlaceIngestion() {
        logger.info("Starting scheduled regional place ingestion job")
        
        try {
            val jobParameters = JobParametersBuilder()
                .addLong("timestamp", System.currentTimeMillis())
                .addString("scheduled", "true")
                .toJobParameters()

            val jobExecution = jobLauncher.run(regionalPlaceIngestionJob, jobParameters)
            
            logger.info("Regional place ingestion job completed with status: {}, execution id: {}", 
                jobExecution.status, jobExecution.id)
                
        } catch (ex: Exception) {
            logger.error("Failed to execute scheduled regional place ingestion job: ${ex.message}", ex)
        }
    }
    
    @Scheduled(cron = "0 0 3 * * ?")  // Daily at 3 AM
    fun runDatabaseCleanup() {
        logger.info("Starting scheduled database cleanup")
        
        try {
            val cleanupResult = databaseCleanupService.cleanupOldAndLowRatedPlaces()
            logger.info("Database cleanup completed. Removed {} places", cleanupResult.removedCount)
        } catch (ex: Exception) {
            logger.error("Failed to execute database cleanup: ${ex.message}", ex)
        }
    }
}