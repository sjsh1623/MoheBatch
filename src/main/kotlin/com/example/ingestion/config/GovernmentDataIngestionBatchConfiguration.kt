package com.example.ingestion.config

import com.example.ingestion.batch.processor.GovernmentRegionProcessor
import com.example.ingestion.batch.reader.GovernmentApiReader
import com.example.ingestion.batch.writer.PlaceWriter
import com.example.ingestion.dto.GovernmentRegionDto
import com.example.ingestion.entity.Place
import org.springframework.batch.core.Job
import org.springframework.batch.core.Step
import org.springframework.batch.core.configuration.annotation.JobScope
import org.springframework.batch.core.job.builder.JobBuilder
import org.springframework.batch.core.repository.JobRepository
import org.springframework.batch.core.step.builder.StepBuilder
import org.springframework.batch.item.support.CompositeItemProcessor
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.transaction.PlatformTransactionManager

@Configuration
class GovernmentDataIngestionBatchConfiguration(
    private val jobRepository: JobRepository,
    private val transactionManager: PlatformTransactionManager,
    private val governmentApiReader: GovernmentApiReader,
    private val governmentRegionProcessor: GovernmentRegionProcessor,
    private val placeWriter: PlaceWriter
) {

    // DISABLED: Korean government regions are now used temporarily by ContinuousPlaceReader
    // No longer need to save government regions to database
    // @Bean
    // fun governmentDataIngestionJob(): Job {
    //     return JobBuilder("governmentDataIngestionJob", jobRepository)
    //         .start(governmentDataIngestionStep())
    //         .build()
    // }

    // DISABLED: Step also disabled since job is disabled
    // @Bean
    // @JobScope
    // fun governmentDataIngestionStep(): Step {
    //     return StepBuilder("governmentDataIngestionStep", jobRepository)
    //         .chunk<GovernmentRegionDto, Place>(1000, transactionManager)
    //         .reader(governmentApiReader)
    //         .processor(governmentRegionProcessor)
    //         .writer(placeWriter)
    //         .build()
    // }
}
