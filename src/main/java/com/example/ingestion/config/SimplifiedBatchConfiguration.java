package com.example.ingestion.config;

import com.example.ingestion.batch.processor.OptimizedPlaceEnrichmentProcessor;
import com.example.ingestion.batch.reader.EnrichedPlace;
import com.example.ingestion.batch.reader.RealApiPlaceReader;
import com.example.ingestion.batch.writer.DatabasePlaceWriter;
import com.example.ingestion.dto.ProcessedPlaceJava;
import com.example.ingestion.service.impl.DatabaseInitializationService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.*;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;
import org.springframework.core.task.TaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.web.reactive.function.client.WebClient;

import java.util.concurrent.CompletableFuture;

/**
 * Simplified batch configuration for Java-only MoheBatch
 */
@Configuration
public class SimplifiedBatchConfiguration {

    private static final Logger logger = LoggerFactory.getLogger(SimplifiedBatchConfiguration.class);

    private final DatabaseInitializationService databaseInitializationService;
    private final int chunkSize;
    private final int skipLimit;

    public SimplifiedBatchConfiguration(
            @Lazy DatabaseInitializationService databaseInitializationService,
            @Value("${app.batch.chunk-size:50}") int chunkSize,
            @Value("${app.batch.skip-limit:10}") int skipLimit
    ) {
        this.databaseInitializationService = databaseInitializationService;
        this.chunkSize = chunkSize;
        this.skipLimit = skipLimit;
    }

    /**
     * Main optimized place ingestion job
     */
    @Bean(name = "optimizedPlaceIngestionJob")
    public Job optimizedPlaceIngestionJob(
            JobRepository jobRepository,
            @Qualifier("databaseInitializationStep") Step databaseInitializationStep,
            @Qualifier("placeEnrichmentStep") Step placeEnrichmentStep
    ) {
        return new JobBuilder("optimizedPlaceIngestionJob", jobRepository)
                .incrementer(new RunIdIncrementer())
                .listener(new JobExecutionListener() {
                    @Override
                    public void beforeJob(JobExecution jobExecution) {
                        logger.info("🚀 Starting optimized place ingestion job with parameters: {}",
                                   jobExecution.getJobParameters());
                    }

                    @Override
                    public void afterJob(JobExecution jobExecution) {
                        long duration = java.time.Duration.between(jobExecution.getStartTime(), jobExecution.getEndTime()).toMillis();
                        logger.info("✅ Completed optimized place ingestion job in {}ms with status: {}",
                                   duration, jobExecution.getStatus());
                    }
                })
                .start(databaseInitializationStep)
                .next(placeEnrichmentStep)
                .build();
    }

    /**
     * Database initialization step
     */
    @Bean(name = "databaseInitializationStep")
    public Step databaseInitializationStep(JobRepository jobRepository,
                                         PlatformTransactionManager transactionManager) {
        return new StepBuilder("databaseInitializationStep", jobRepository)
                .tasklet(databaseInitializationTasklet(), transactionManager)
                .build();
    }

    /**
     * Main place enrichment step
     */
    @Bean(name = "placeEnrichmentStep")
    public Step placeEnrichmentStep(
            JobRepository jobRepository,
            PlatformTransactionManager transactionManager,
            RealApiPlaceReader reader,
            OptimizedPlaceEnrichmentProcessor processor,
            DatabasePlaceWriter writer
    ) {
        return new StepBuilder("placeEnrichmentStep", jobRepository)
                .<EnrichedPlace, ProcessedPlaceJava>chunk(chunkSize, transactionManager)
                .reader(reader)
                .processor(processor)
                .writer(writer)
                .faultTolerant()
                .skipLimit(skipLimit)
                .skip(Exception.class)
                .taskExecutor(placeProcessingTaskExecutor())
                .listener(new StepExecutionListener() {
                    @Override
                    public void beforeStep(StepExecution stepExecution) {
                        logger.info("🔄 Starting place enrichment step with chunk size: {}", chunkSize);
                    }

                    @Override
                    public ExitStatus afterStep(StepExecution stepExecution) {
                        logger.info("📊 Place enrichment step completed - Read: {}, Written: {}, Skipped: {}",
                                   stepExecution.getReadCount(),
                                   stepExecution.getWriteCount(),
                                   stepExecution.getSkipCount());
                        return ExitStatus.COMPLETED;
                    }
                })
                .build();
    }

    /**
     * Database initialization tasklet
     */
    @Bean
    public Tasklet databaseInitializationTasklet() {
        return (contribution, chunkContext) -> {
            logger.info("🗃️ Initializing database - clearing existing place data");

            try {
                CompletableFuture<DatabaseInitializationService.DatabaseInitializationResult> future =
                        databaseInitializationService.initializePlaceDatabase();

                DatabaseInitializationService.DatabaseInitializationResult result = future.get();

                if (result.isSuccess()) {
                    logger.info("✅ Database initialization completed: {}", result.getMessage());
                    logger.info("📊 Cleared {} places and {} vectors",
                               result.getPlacesCleared(), result.getVectorsCleared());
                } else {
                    logger.warn("⚠️ Database initialization skipped or failed: {}", result.getMessage());
                    // Don't fail the job, just continue with mock processing
                }

                return RepeatStatus.FINISHED;

            } catch (Exception e) {
                logger.warn("⚠️ Database initialization error (continuing with mock processing): {}", e.getMessage());
                return RepeatStatus.FINISHED; // Continue anyway for testing
            }
        };
    }

    /**
     * Task executor for place processing
     */
    @Bean(name = "placeProcessingTaskExecutor")
    public TaskExecutor placeProcessingTaskExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(2);
        executor.setMaxPoolSize(4);
        executor.setQueueCapacity(50);
        executor.setThreadNamePrefix("PlaceProcessor-");
        executor.setKeepAliveSeconds(60);
        executor.setRejectedExecutionHandler(new java.util.concurrent.ThreadPoolExecutor.CallerRunsPolicy());
        executor.initialize();

        logger.info("⚙️ Initialized place processing task executor with core: 2, max: 4");
        return executor;
    }

    /**
     * Batch async executor
     */
    @Bean(name = "batchAsyncExecutor")
    public TaskExecutor batchAsyncExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(1);
        executor.setMaxPoolSize(2);
        executor.setQueueCapacity(10);
        executor.setThreadNamePrefix("BatchAsync-");
        executor.setKeepAliveSeconds(30);
        executor.initialize();

        logger.info("⚙️ Initialized batch async executor with core: 1, max: 2");
        return executor;
    }

    /**
     * WebClient for HTTP calls
     */
    @Bean
    public WebClient webClient() {
        return WebClient.builder()
                .codecs(configurer -> configurer.defaultCodecs().maxInMemorySize(10 * 1024 * 1024)) // 10MB
                .build();
    }
}