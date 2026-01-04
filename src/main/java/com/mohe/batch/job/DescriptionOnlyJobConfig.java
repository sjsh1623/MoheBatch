package com.mohe.batch.job;

import com.mohe.batch.entity.Place;
import com.mohe.batch.entity.PlaceDescription;
import com.mohe.batch.entity.PlaceReview;
import com.mohe.batch.repository.PlaceRepository;
import com.mohe.batch.service.OpenAiDescriptionService;
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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

/**
 * Description 전용 배치 Job
 * - 크롤링 완료된 장소 중 mohe_description이 없는 경우
 * - 리뷰 데이터만 가지고 OpenAI에게 description 생성 요청
 */
@Configuration
public class DescriptionOnlyJobConfig {

    private static final Logger log = LoggerFactory.getLogger(DescriptionOnlyJobConfig.class);

    private final OpenAiDescriptionService openAiDescriptionService;
    private final PlaceRepository placeRepository;

    @Value("${batch.chunk-size:10}")
    private int chunkSize;

    public DescriptionOnlyJobConfig(
            OpenAiDescriptionService openAiDescriptionService,
            PlaceRepository placeRepository
    ) {
        this.openAiDescriptionService = openAiDescriptionService;
        this.placeRepository = placeRepository;
    }

    @Bean(name = "descriptionOnlyJob")
    public Job descriptionOnlyJob(
            JobRepository jobRepository,
            @Qualifier("descriptionOnlyStep") Step descriptionOnlyStep
    ) {
        return new JobBuilder("descriptionOnlyJob", jobRepository)
                .start(descriptionOnlyStep)
                .build();
    }

    @Bean(name = "descriptionOnlyStep")
    public Step descriptionOnlyStep(
            JobRepository jobRepository,
            PlatformTransactionManager transactionManager,
            @Qualifier("descriptionOnlyReader") ItemReader<Place> descriptionOnlyReader,
            @Qualifier("descriptionOnlyProcessor") ItemProcessor<Place, Place> descriptionOnlyProcessor,
            @Qualifier("descriptionOnlyWriter") ItemWriter<Place> descriptionOnlyWriter,
            @Qualifier("batchTaskExecutor") TaskExecutor batchTaskExecutor
    ) {
        // AsyncItemProcessor 설정
        AsyncItemProcessor<Place, Place> asyncItemProcessor = new AsyncItemProcessor<>();
        asyncItemProcessor.setDelegate(descriptionOnlyProcessor);
        asyncItemProcessor.setTaskExecutor(batchTaskExecutor);

        // AsyncItemWriter 설정
        AsyncItemWriter<Place> asyncItemWriter = new AsyncItemWriter<>();
        asyncItemWriter.setDelegate(descriptionOnlyWriter);

        try {
            asyncItemProcessor.afterPropertiesSet();
            asyncItemWriter.afterPropertiesSet();
        } catch (Exception e) {
            throw new RuntimeException("Failed to initialize async processors", e);
        }

        log.info("Description-only batch step configured: chunkSize={}", chunkSize);

        return new StepBuilder("descriptionOnlyStep", jobRepository)
                .<Place, Future<Place>>chunk(chunkSize, transactionManager)
                .reader(descriptionOnlyReader)
                .processor(asyncItemProcessor)
                .writer(asyncItemWriter)
                .faultTolerant()
                .skip(Exception.class)
                .skipLimit(Integer.MAX_VALUE)
                .noRollback(Exception.class)
                .build();
    }

    @Bean(name = "descriptionOnlyReader")
    @StepScope
    public ItemReader<Place> descriptionOnlyReader() {
        log.info("Creating DescriptionOnlyReader");
        return new DescriptionOnlyReader(placeRepository, 50);
    }

    @Bean(name = "descriptionOnlyProcessor")
    public ItemProcessor<Place, Place> descriptionOnlyProcessor() {
        return place -> {
            try {
                log.info("🔄 Description 생성 시작 '{}' (ID: {})", place.getName(), place.getId());

                // 리뷰 데이터 수집
                List<PlaceReview> reviews = place.getReviews();
                if (reviews == null || reviews.isEmpty()) {
                    log.warn("⚠️ 리뷰가 없어서 스킵 '{}' (ID: {})", place.getName(), place.getId());
                    return null; // 리뷰 없으면 처리 안함
                }

                // 리뷰 텍스트 준비 (최대 10개)
                int reviewLimit = Math.min(reviews.size(), 10);
                String reviewsForPrompt = reviews.stream()
                        .limit(reviewLimit)
                        .map(PlaceReview::getReviewText)
                        .filter(text -> text != null && !text.trim().isEmpty())
                        .collect(Collectors.joining("\n"));

                if (reviewsForPrompt.trim().isEmpty()) {
                    log.warn("⚠️ 유효한 리뷰 텍스트가 없어서 스킵 '{}' (ID: {})", place.getName(), place.getId());
                    return null;
                }

                // 카테고리 문자열 준비
                String categoryStr = place.getCategory() != null ? String.join(",", place.getCategory()) : "";

                // OpenAI 요청 페이로드 생성 (리뷰만 사용)
                OpenAiDescriptionService.DescriptionPayload payload =
                        new OpenAiDescriptionService.DescriptionPayload(
                                "",  // ai_summary 없음
                                reviewsForPrompt,  // 리뷰 데이터
                                "",  // original_description 없음
                                categoryStr,
                                place.getPetFriendly() != null && place.getPetFriendly()
                        );

                // OpenAI API 호출
                OpenAiDescriptionService.DescriptionResult descriptionResult =
                        openAiDescriptionService.generateDescription(payload).orElse(null);

                if (descriptionResult == null || descriptionResult.description() == null
                        || descriptionResult.description().trim().isEmpty()) {
                    log.warn("⚠️ OpenAI 응답 없음 '{}' (ID: {})", place.getName(), place.getId());
                    return null;
                }

                String moheDescription = descriptionResult.description();
                List<String> keywords = descriptionResult.keywords();

                // PlaceDescription 업데이트 또는 생성
                PlaceDescription description;
                if (place.getDescriptions().isEmpty()) {
                    description = new PlaceDescription();
                    description.setPlace(place);
                    place.getDescriptions().add(description);
                } else {
                    description = place.getDescriptions().get(0);
                }

                description.setMoheDescription(moheDescription);

                // 키워드 업데이트
                if (keywords != null && keywords.size() == 9) {
                    place.setKeyword(keywords);
                } else {
                    // Fallback 키워드
                    List<String> fallbackKeywords = new ArrayList<>();
                    if (place.getCategory() != null && !place.getCategory().isEmpty()) {
                        fallbackKeywords.addAll(place.getCategory());
                    }
                    while (fallbackKeywords.size() < 9) {
                        fallbackKeywords.add("장소");
                    }
                    place.setKeyword(fallbackKeywords.subList(0, 9));
                }

                log.info("✅ Description 생성 완료 '{}' (ID: {}) - {}자",
                        place.getName(), place.getId(), moheDescription.length());

                return place;

            } catch (Exception e) {
                log.error("❌ Description 생성 실패 '{}' (ID: {}): {}",
                        place.getName(), place.getId(), e.getMessage());
                return null;
            }
        };
    }

    @Bean(name = "descriptionOnlyWriter")
    public ItemWriter<Place> descriptionOnlyWriter() {
        return chunk -> {
            log.info("Saving {} places with new descriptions...", chunk.getItems().size());
            int savedCount = 0;

            for (Place place : chunk.getItems()) {
                if (place == null) continue;

                try {
                    // Fresh entity 조회
                    Place freshPlace = placeRepository.findByIdWithDescriptions(place.getId())
                            .orElseThrow(() -> new IllegalStateException("Place not found: " + place.getId()));

                    // Description 업데이트
                    if (!place.getDescriptions().isEmpty()) {
                        PlaceDescription newDesc = place.getDescriptions().get(0);

                        if (freshPlace.getDescriptions().isEmpty()) {
                            // 새 Description 추가
                            PlaceDescription desc = new PlaceDescription();
                            desc.setPlace(freshPlace);
                            desc.setMoheDescription(newDesc.getMoheDescription());
                            freshPlace.getDescriptions().add(desc);
                        } else {
                            // 기존 Description 업데이트
                            PlaceDescription existingDesc = freshPlace.getDescriptions().get(0);
                            existingDesc.setMoheDescription(newDesc.getMoheDescription());
                        }
                    }

                    // 키워드 업데이트
                    if (place.getKeyword() != null && !place.getKeyword().isEmpty()) {
                        freshPlace.setKeyword(place.getKeyword());
                    }

                    placeRepository.saveAndFlush(freshPlace);
                    savedCount++;

                    log.info("💾 저장 완료 '{}' (ID: {})", freshPlace.getName(), freshPlace.getId());

                } catch (Exception e) {
                    log.error("❌ 저장 실패 Place ID {}: {}", place.getId(), e.getMessage());
                }
            }

            log.info("✅ Successfully saved {}/{} places", savedCount, chunk.getItems().size());
        };
    }
}
