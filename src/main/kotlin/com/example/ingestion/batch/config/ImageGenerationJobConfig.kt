package com.example.ingestion.batch.config

import com.example.ingestion.batch.processor.ImageGenerationProcessor
import com.example.ingestion.batch.writer.PlaceImageWriter
import com.example.ingestion.config.ImageGenerationProperties
import com.example.ingestion.entity.Place
import org.springframework.batch.core.Job
import org.springframework.batch.core.Step
import org.springframework.batch.core.job.builder.JobBuilder
import org.springframework.batch.core.repository.JobRepository
import org.springframework.batch.core.step.builder.StepBuilder
import org.springframework.batch.item.database.JpaPagingItemReader
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.core.task.SimpleAsyncTaskExecutor
import org.springframework.core.task.TaskExecutor
import org.springframework.transaction.PlatformTransactionManager

@Configuration
class ImageGenerationJobConfig {

    @Bean
    fun placeImageGenerationJob(
        jobRepository: JobRepository,
        placeImageGenerationStep: Step
    ): Job {
        return JobBuilder("placeImageGenerationJob", jobRepository)
            .start(placeImageGenerationStep)
            .build()
    }

    @Bean
    fun placeImageGenerationStep(
        jobRepository: JobRepository,
        transactionManager: PlatformTransactionManager,
        placeImageItemReader: JpaPagingItemReader<Place>,
        placeImageProcessor: ImageGenerationProcessor,
        placeImageWriter: PlaceImageWriter,
        properties: ImageGenerationProperties
    ): Step {
        return StepBuilder("placeImageGenerationStep", jobRepository)
            .chunk<Place, Place>(10, transactionManager)
            .reader(placeImageItemReader)
            .processor(placeImageProcessor)
            .writer(placeImageWriter)
            .taskExecutor(taskExecutor(properties.concurrency))
            .build()
    }

    @Bean
    fun taskExecutor(concurrency: Int): TaskExecutor {
        val asyncTaskExecutor = SimpleAsyncTaskExecutor("spring_batch")
        asyncTaskExecutor.concurrencyLimit = concurrency
        return asyncTaskExecutor
    }
}
