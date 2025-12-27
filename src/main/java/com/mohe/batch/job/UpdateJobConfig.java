package com.mohe.batch.job;

import com.mohe.batch.entity.*;
import com.mohe.batch.repository.PlaceRepository;
import com.mohe.batch.service.UpdateProcessorService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.integration.async.AsyncItemProcessor;
import org.springframework.batch.integration.async.AsyncItemWriter;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.TaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;

import java.util.concurrent.Future;

/**
 * 업데이트 전용 Job 설정
 * - OpenAI 없이 메뉴/이미지/리뷰만 업데이트
 * - 리뷰 중복 체크 (앞 10글자 동일시 skip)
 */
@Configuration
public class UpdateJobConfig {

    private static final Logger log = LoggerFactory.getLogger(UpdateJobConfig.class);

    private final PlaceRepository placeRepository;
    private final UpdateProcessorService updateProcessorService;

    @Value("${batch.chunk-size:10}")
    private int chunkSize;

    @Value("${batch.worker.total-workers:3}")
    private int totalWorkers;

    public UpdateJobConfig(
            PlaceRepository placeRepository,
            UpdateProcessorService updateProcessorService
    ) {
        this.placeRepository = placeRepository;
        this.updateProcessorService = updateProcessorService;
    }

    @Bean(name = "updateJob")
    public Job updateJob(JobRepository jobRepository, @Qualifier("updateStep") Step updateStep) {
        return new JobBuilder("updateJob", jobRepository)
                .start(updateStep)
                .build();
    }

    @Bean(name = "updateStep")
    public Step updateStep(
            JobRepository jobRepository,
            PlatformTransactionManager transactionManager,
            @Qualifier("updateReader") ItemReader<Place> updateReader,
            @Qualifier("updateProcessor") ItemProcessor<Place, Place> updateProcessor,
            @Qualifier("updateWriter") ItemWriter<Place> updateWriter,
            @Qualifier("batchTaskExecutor") TaskExecutor batchTaskExecutor
    ) {
        AsyncItemProcessor<Place, Place> asyncItemProcessor = new AsyncItemProcessor<>();
        asyncItemProcessor.setDelegate(updateProcessor);
        asyncItemProcessor.setTaskExecutor(batchTaskExecutor);

        AsyncItemWriter<Place> asyncItemWriter = new AsyncItemWriter<>();
        asyncItemWriter.setDelegate(updateWriter);

        try {
            asyncItemProcessor.afterPropertiesSet();
            asyncItemWriter.afterPropertiesSet();
        } catch (Exception e) {
            throw new RuntimeException("Failed to initialize async processors", e);
        }

        return new StepBuilder("updateStep", jobRepository)
                .<Place, Future<Place>>chunk(chunkSize, transactionManager)
                .reader(updateReader)
                .processor(asyncItemProcessor)
                .writer(asyncItemWriter)
                .faultTolerant()
                .skip(Exception.class)
                .skipLimit(Integer.MAX_VALUE)
                .noRollback(Exception.class)
                .build();
    }

    @Bean(name = "updateReader")
    @StepScope
    public ItemReader<Place> updateReader(
            @Value("#{jobParameters['workerId']}") Long workerId
    ) {
        int workerIdInt = workerId != null ? workerId.intValue() : 0;
        log.info("🔄 UpdateReader 생성: worker {}", workerIdInt);
        // crawler_found=true인 장소만 읽기 (이미 크롤링된 데이터 업데이트)
        return new UpdateReader(placeRepository, workerIdInt, totalWorkers, 10);
    }

    @Bean(name = "updateProcessor")
    @StepScope
    public ItemProcessor<Place, Place> updateProcessor(
            @Value("#{jobParameters['updateMenus']}") String updateMenusStr,
            @Value("#{jobParameters['updateImages']}") String updateImagesStr,
            @Value("#{jobParameters['updateReviews']}") String updateReviewsStr
    ) {
        boolean updateMenus = Boolean.parseBoolean(updateMenusStr);
        boolean updateImages = Boolean.parseBoolean(updateImagesStr);
        boolean updateReviews = Boolean.parseBoolean(updateReviewsStr);

        log.info("🔄 UpdateProcessor 설정: menus={}, images={}, reviews={}",
                updateMenus, updateImages, updateReviews);

        return place -> {
            // UpdateProcessorService에서 트랜잭션 내에서 처리
            // place.getId()만 전달하여 서비스에서 fresh한 엔티티 조회
            return updateProcessorService.processUpdate(
                    place.getId(),
                    updateMenus,
                    updateImages,
                    updateReviews
            );
        };
    }

    @Bean(name = "updateWriter")
    public ItemWriter<Place> updateWriter() {
        return chunk -> {
            // UpdateProcessorService에서 이미 저장까지 완료됨
            // Writer는 로그만 출력
            log.info("💾 Chunk 처리 완료: {}개 장소", chunk.getItems().size());
        };
    }
}
