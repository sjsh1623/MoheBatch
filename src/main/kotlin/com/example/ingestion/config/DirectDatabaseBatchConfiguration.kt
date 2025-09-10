package com.example.ingestion.config

import com.example.ingestion.batch.reader.DirectDatabasePlaceReader
import com.example.ingestion.batch.reader.EnrichedPlace
import com.example.ingestion.batch.processor.RegionalPlaceEnrichmentProcessor
import com.example.ingestion.batch.writer.MoheSpringApiWriter
import com.example.ingestion.dto.ProcessedPlace
import org.springframework.batch.core.Job
import org.springframework.batch.core.Step
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing
import org.springframework.batch.core.configuration.support.DefaultBatchConfiguration
import org.springframework.batch.core.job.builder.JobBuilder
import org.springframework.batch.core.launch.support.RunIdIncrementer
import org.springframework.batch.core.repository.JobRepository
import org.springframework.batch.core.step.builder.StepBuilder
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.transaction.PlatformTransactionManager
import org.slf4j.LoggerFactory

/**
 * Direct Database Batch Configuration - API Bypass Solution
 * 
 * This configuration creates a batch job that completely bypasses all external APIs
 * and directly generates realistic Korean place data for continuous database storage.
 * 
 * Use this when:
 * - External API credentials are not available
 * - API rate limits are exceeded  
 * - You need immediate continuous data flow
 * - Testing database pipeline without API dependencies
 */
@Configuration
@EnableBatchProcessing
class DirectDatabaseBatchConfiguration : DefaultBatchConfiguration() {

    private val logger = LoggerFactory.getLogger(DirectDatabaseBatchConfiguration::class.java)

    @Bean
    fun directDatabasePlaceIngestionJob(
        jobRepository: JobRepository,
        directDatabasePlaceStep: Step
    ): Job {
        logger.info("🚀 Creating Direct Database Place Ingestion Job (API Bypass)")
        
        return JobBuilder("directDatabasePlaceIngestionJob", jobRepository)
            .incrementer(RunIdIncrementer())
            .start(directDatabasePlaceStep)
            .build()
    }

    @Bean
    fun directDatabasePlaceStep(
        jobRepository: JobRepository,
        transactionManager: PlatformTransactionManager,
        directDatabasePlaceReader: DirectDatabasePlaceReader,
        regionalPlaceEnrichmentProcessor: RegionalPlaceEnrichmentProcessor,
        moheSpringApiWriter: MoheSpringApiWriter
    ): Step {
        logger.info("🔧 Creating Direct Database Place Step (Chunk Size: 20)")
        
        return StepBuilder("directDatabasePlaceStep", jobRepository)
            .chunk<EnrichedPlace, ProcessedPlace>(20, transactionManager)
            .reader(directDatabasePlaceReader)
            .processor(regionalPlaceEnrichmentProcessor)
            .writer(moheSpringApiWriter)
            .build()
    }
}