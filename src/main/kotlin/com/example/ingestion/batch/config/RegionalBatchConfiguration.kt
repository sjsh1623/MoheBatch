package com.example.ingestion.batch.config

import com.example.ingestion.batch.listener.JobExecutionListener
import com.example.ingestion.batch.processor.RegionalPlaceEnrichmentProcessor
import com.example.ingestion.batch.reader.ContinuousPlaceReader
import com.example.ingestion.batch.reader.EnrichedPlace
import com.example.ingestion.config.ContinuousBatchService
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

@Configuration
@EnableRetry
class RegionalBatchConfiguration(
    @Value("\${app.batch.chunk-size:500}") private val chunkSize: Int,
    @Value("\${app.batch.skip-limit:100}") private val skipLimit: Int
) {

    private val logger = LoggerFactory.getLogger(RegionalBatchConfiguration::class.java)

    @Bean
    fun regionalPlaceIngestionJob(
        jobRepository: JobRepository,
        regionalPlaceStep: Step,
        jobExecutionListener: JobExecutionListener,
        continuousBatchService: ContinuousBatchService?
    ): Job {
        val jobBuilder = JobBuilder("regionalPlaceIngestionJob", jobRepository)
            .incrementer(RunIdIncrementer())
            .listener(jobExecutionListener)
            
        // Add continuous batch service as listener if available
        continuousBatchService?.let {
            jobBuilder.listener(it)
            logger.info("Added ContinuousBatchService as job listener")
        }
            
        return jobBuilder
            .start(regionalPlaceStep)
            .build()
    }

    @Bean
    fun regionalPlaceStep(
        jobRepository: JobRepository,
        transactionManager: PlatformTransactionManager,
        continuousPlaceReader: ContinuousPlaceReader,
        regionalPlaceEnrichmentProcessor: RegionalPlaceEnrichmentProcessor,
        moheSpringApiWriter: MoheSpringApiWriter
    ): Step {
        logger.info("Configuring Regional place ingestion step with chunk size: $chunkSize, skip limit: $skipLimit")
        
        return StepBuilder("regionalPlaceStep", jobRepository)
            .chunk<EnrichedPlace, ProcessedPlace>(chunkSize, transactionManager)
            .reader(continuousPlaceReader)
            .processor(regionalPlaceEnrichmentProcessor)
            .writer(moheSpringApiWriter)
            .faultTolerant()
            .skipPolicy(regionalCustomSkipPolicy())
            .skipLimit(skipLimit)
            .retry(WebClientResponseException::class.java)
            .retryLimit(3)
            .retry(SocketTimeoutException::class.java)
            .retryLimit(2)
            .build()
    }

    @Bean
    fun regionalCustomSkipPolicy(): SkipPolicy {
        return SkipPolicy { throwable, skipCount ->
            when (throwable) {
                is IllegalArgumentException -> {
                    logger.warn("Skipping item due to validation error: ${throwable.message}")
                    true
                }
                is NumberFormatException -> {
                    logger.warn("Skipping item due to number format error: ${throwable.message}")
                    true
                }
                is java.time.format.DateTimeParseException -> {
                    logger.warn("Skipping item due to date parsing error: ${throwable.message}")
                    true
                }
                is org.springframework.dao.DataAccessException -> {
                    logger.error("Database error, not skipping: ${throwable.message}")
                    false
                }
                is WebClientResponseException -> {
                    logger.error("API error, not skipping (should retry): ${throwable.message}")
                    false
                }
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
}