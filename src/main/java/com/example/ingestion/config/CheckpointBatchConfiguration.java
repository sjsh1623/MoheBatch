package com.example.ingestion.config;

import com.example.ingestion.batch.processor.OptimizedPlaceEnrichmentProcessor;
import com.example.ingestion.batch.reader.CheckpointAwarePlaceReader;
import com.example.ingestion.batch.reader.EnrichedPlace;
import com.example.ingestion.batch.writer.DatabasePlaceWriter;
import com.example.ingestion.dto.ProcessedPlaceJava;
import com.example.ingestion.service.CheckpointService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.*;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.core.task.TaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

/**
 * 체크포인트 기반 배치 설정
 * 정부 API를 기준으로 중단된 지점부터 재시작 가능한 배치 구현
 */
@Configuration
public class CheckpointBatchConfiguration {

    private static final Logger logger = LoggerFactory.getLogger(CheckpointBatchConfiguration.class);

    @Autowired
    private CheckpointService checkpointService;

    // Configuration values
    private final int chunkSize;
    private final int skipLimit;

    public CheckpointBatchConfiguration(
            @Value("${app.batch.chunk-size:10}") int chunkSize,
            @Value("${app.batch.skip-limit:50}") int skipLimit
    ) {
        this.chunkSize = chunkSize;
        this.skipLimit = skipLimit;
    }

    /**
     * 체크포인트 기반 장소 수집 배치 잡
     * 기존 optimizedPlaceIngestionJob을 대체
     */
    @Primary
    @Bean(name = "optimizedPlaceIngestionJob")
    public Job checkpointPlaceIngestionJob(
            JobRepository jobRepository,
            @Qualifier("checkpointInitializationStep") Step checkpointInitializationStep,
            @Qualifier("checkpointPlaceEnrichmentStep") Step checkpointPlaceEnrichmentStep
    ) {
        return new JobBuilder("checkpoint-place-ingestion-job", jobRepository)
                .incrementer(new RunIdIncrementer())
                .listener(new JobExecutionListener() {
                    @Override
                    public void beforeJob(JobExecution jobExecution) {
                        logger.info("🚀 체크포인트 기반 배치 시작 - 매개변수: {}",
                                   jobExecution.getJobParameters());

                        // 중단된 배치가 있는지 확인
                        if (checkpointService.hasInterruptedBatch("place-ingestion-batch")) {
                            logger.info("🔄 중단된 배치 발견 - 마지막 체크포인트부터 재시작합니다");
                        }
                    }

                    @Override
                    public void afterJob(JobExecution jobExecution) {
                        long duration = java.time.Duration.between(
                                jobExecution.getStartTime(), jobExecution.getEndTime()).toMillis();

                        if (jobExecution.getStatus() == BatchStatus.COMPLETED) {
                            logger.info("🎉 체크포인트 배치 완료 - {}ms, 상태: {}",
                                       duration, jobExecution.getStatus());
                        } else if (jobExecution.getStatus() == BatchStatus.FAILED) {
                            logger.error("💥 체크포인트 배치 실패 - {}ms, 상태: {}",
                                        duration, jobExecution.getStatus());
                        } else {
                            logger.warn("⏸️ 체크포인트 배치 중단 - {}ms, 상태: {}",
                                       duration, jobExecution.getStatus());
                        }

                        // 진행 상태 로깅
                        CheckpointService.BatchProgress progress =
                            checkpointService.getBatchProgress("place-ingestion-batch");
                        logger.info("📊 최종 진행 상태: {}", progress);
                    }
                })
                .start(checkpointInitializationStep)
                .next(checkpointPlaceEnrichmentStep)
                .build();
    }

    /**
     * 체크포인트 시스템 초기화 스텝
     * 데이터베이스 초기화 대신 체크포인트 테이블 준비
     */
    @Bean(name = "checkpointInitializationStep")
    public Step checkpointInitializationStep(JobRepository jobRepository,
                                            PlatformTransactionManager transactionManager) {
        return new StepBuilder("checkpointInitializationStep", jobRepository)
                .tasklet(checkpointInitializationTasklet(), transactionManager)
                .build();
    }

    /**
     * 체크포인트 기반 장소 수집 스텝
     * CheckpointAwarePlaceReader 사용
     */
    @Bean(name = "checkpointPlaceEnrichmentStep")
    public Step checkpointPlaceEnrichmentStep(
            JobRepository jobRepository,
            PlatformTransactionManager transactionManager,
            CheckpointAwarePlaceReader checkpointReader,
            OptimizedPlaceEnrichmentProcessor processor,
            DatabasePlaceWriter writer
    ) {
        return new StepBuilder("checkpointPlaceEnrichmentStep", jobRepository)
                .<EnrichedPlace, ProcessedPlaceJava>chunk(chunkSize, transactionManager)
                .reader(checkpointReader)
                .processor(processor)
                .writer(writer)
                .faultTolerant()
                .skipLimit(skipLimit)
                .skip(Exception.class)
                .taskExecutor(checkpointTaskExecutor())
                .listener(new StepExecutionListener() {
                    @Override
                    public void beforeStep(StepExecution stepExecution) {
                        logger.info("🔄 체크포인트 기반 장소 수집 시작 - 청크 크기: {}", chunkSize);

                        // 진행 상태 출력
                        CheckpointService.BatchProgress progress =
                            checkpointService.getBatchProgress("place-ingestion-batch");
                        logger.info("📊 현재 진행 상태: {}", progress);
                    }

                    @Override
                    public ExitStatus afterStep(StepExecution stepExecution) {
                        logger.info("📊 체크포인트 스텝 완료 - 읽음: {}, 처리됨: {}, 건너뜀: {}",
                                   stepExecution.getReadCount(),
                                   stepExecution.getWriteCount(),
                                   stepExecution.getSkipCount());

                        // 최종 진행 상태 출력
                        CheckpointService.BatchProgress progress =
                            checkpointService.getBatchProgress("place-ingestion-batch");
                        logger.info("📊 최종 진행 상태: {}", progress);

                        return ExitStatus.COMPLETED;
                    }
                })
                .build();
    }

    /**
     * 체크포인트 초기화 태스클릿
     * 기존 DatabaseInitializationTasklet 대신 사용
     */
    @Bean
    public Tasklet checkpointInitializationTasklet() {
        return (contribution, chunkContext) -> {
            logger.info("🗃️ 체크포인트 시스템 초기화 중...");

            try {
                // 체크포인트 테이블 생성 (이미 존재하면 무시)
                logger.info("📋 체크포인트 테이블 상태 확인 중...");

                // 현재 진행 상태 조회
                CheckpointService.BatchProgress progress =
                    checkpointService.getBatchProgress("place-ingestion-batch");

                if (progress.getTotal() > 0) {
                    logger.info("🔄 기존 체크포인트 발견 - 진행 상태: {}", progress);
                    logger.info("📍 중단된 지점부터 재시작합니다");
                } else {
                    logger.info("🆕 새로운 배치 시작 - 정부 API에서 지역 목록을 가져옵니다");
                }

                logger.info("✅ 체크포인트 시스템 초기화 완료");
                return RepeatStatus.FINISHED;

            } catch (Exception e) {
                logger.error("❌ 체크포인트 초기화 실패", e);
                throw new RuntimeException("체크포인트 초기화 실패", e);
            }
        };
    }

    /**
     * 체크포인트 전용 태스크 실행자
     */
    @Bean(name = "checkpointTaskExecutor")
    public TaskExecutor checkpointTaskExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(1); // 체크포인트는 순차 처리
        executor.setMaxPoolSize(2);
        executor.setQueueCapacity(20);
        executor.setThreadNamePrefix("CheckpointProcessor-");
        executor.setKeepAliveSeconds(60);
        executor.setRejectedExecutionHandler(
            new java.util.concurrent.ThreadPoolExecutor.CallerRunsPolicy());
        executor.initialize();

        logger.info("⚙️ 체크포인트 태스크 실행자 초기화 - 코어: 1, 최대: 2");
        return executor;
    }

    /**
     * 체크포인트 모니터링 태스크 실행자
     */
    @Bean(name = "checkpointMonitorExecutor")
    public TaskExecutor checkpointMonitorExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(1);
        executor.setMaxPoolSize(1);
        executor.setQueueCapacity(5);
        executor.setThreadNamePrefix("CheckpointMonitor-");
        executor.setKeepAliveSeconds(30);
        executor.initialize();

        logger.info("⚙️ 체크포인트 모니터 실행자 초기화");
        return executor;
    }
}