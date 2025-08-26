package com.example.ingestion.config

import com.example.ingestion.batch.listener.JobExecutionListener
import com.example.ingestion.batch.processor.DataProcessor
// import com.example.ingestion.batch.reader.NaverGooglePlaceReader
import com.example.ingestion.batch.reader.EnrichedPlace
import com.example.ingestion.batch.processor.PlaceEnrichmentProcessor
import com.example.ingestion.dto.ProcessedPlace
import com.example.ingestion.batch.writer.MoheSpringApiWriter
import org.slf4j.LoggerFactory
import org.springframework.batch.core.Job
import org.springframework.batch.core.Step
import org.springframework.batch.core.job.builder.JobBuilder
import org.springframework.batch.core.launch.support.RunIdIncrementer
import org.springframework.batch.core.repository.JobRepository
import org.springframework.batch.core.step.builder.StepBuilder
import org.springframework.batch.core.step.skip.SkipPolicy
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.retry.annotation.EnableRetry
import org.springframework.transaction.PlatformTransactionManager
import org.springframework.web.reactive.function.client.WebClientResponseException
import java.net.SocketTimeoutException

// DISABLED - OLD BROKEN CONFIGURATION - Use RegionalBatchConfiguration instead
/* @Configuration
@EnableRetry
class BatchConfiguration(
    @Value("\${app.batch.chunk-size:500}") private val chunkSize: Int,
    @Value("\${app.batch.skip-limit:100}") private val skipLimit: Int
) {

    private val logger = LoggerFactory.getLogger(BatchConfiguration::class.java)

    @Bean
    fun naverGooglePlaceIngestionJob(
        jobRepository: JobRepository,
        naverGooglePlaceStep: Step,
        jobExecutionListener: JobExecutionListener
    ): Job {
        return JobBuilder("naverGooglePlaceIngestionJob", jobRepository)
            .incrementer(RunIdIncrementer())
            .listener(jobExecutionListener)
            .start(naverGooglePlaceStep)
            .build()
    }

    @Bean
    fun naverGooglePlaceStep(
        jobRepository: JobRepository,
        transactionManager: PlatformTransactionManager,
        naverGooglePlaceReader: NaverGooglePlaceReader,
        placeEnrichmentProcessor: PlaceEnrichmentProcessor,
        moheSpringApiWriter: MoheSpringApiWriter
    ): Step {
        logger.info("Configuring Naver+Google place ingestion step with chunk size: $chunkSize, skip limit: $skipLimit")
        
        return StepBuilder("naverGooglePlaceStep", jobRepository)
            .chunk<EnrichedPlace, ProcessedPlace>(chunkSize, transactionManager)
            .reader(naverGooglePlaceReader)
            .processor(placeEnrichmentProcessor)
            .writer(moheSpringApiWriter)
            .faultTolerant()
            .skipPolicy(customSkipPolicy())
            .skipLimit(skipLimit)
            .retry(WebClientResponseException::class.java)
            .retryLimit(3)
            .retry(SocketTimeoutException::class.java)
            .retryLimit(2)
            .build()
    }

    @Bean
    fun customSkipPolicy(): SkipPolicy {
        return SkipPolicy { throwable, skipCount ->
            when (throwable) {
                // Skip validation errors from processor
                is IllegalArgumentException -> {
                    logger.warn("Skipping item due to validation error: ${throwable.message}")
                    true
                }
                // Skip items with data format issues
                is NumberFormatException -> {
                    logger.warn("Skipping item due to number format error: ${throwable.message}")
                    true
                }
                // Skip items with date parsing issues
                is java.time.format.DateTimeParseException -> {
                    logger.warn("Skipping item due to date parsing error: ${throwable.message}")
                    true
                }
                // Don't skip database errors - they should cause the job to fail
                is org.springframework.dao.DataAccessException -> {
                    logger.error("Database error, not skipping: ${throwable.message}")
                    false
                }
                // Don't skip connectivity issues - retry instead
                is WebClientResponseException -> {
                    logger.error("API error, not skipping (should retry): ${throwable.message}")
                    false
                }
                // Skip other runtime exceptions but log them
                is RuntimeException -> {
                    logger.warn("Skipping item due to runtime exception (skip count: $skipCount): ${throwable.message}")
                    skipCount < skipLimit
                }
                else -> {
                    logger.error("Unexpected error, not skipping: ${throwable.message}")
                    false
                }
            }
        }
    }
} */