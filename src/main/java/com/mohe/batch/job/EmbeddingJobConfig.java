package com.mohe.batch.job;

import com.mohe.batch.dto.embedding.EmbeddingResponse;
import com.mohe.batch.entity.EmbedStatus;
import com.mohe.batch.entity.Place;
import com.mohe.batch.entity.PlaceKeywordEmbedding;
import com.mohe.batch.repository.PlaceKeywordEmbeddingRepository;
import com.mohe.batch.repository.PlaceRepository;
import com.mohe.batch.service.EmbeddingClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.PlatformTransactionManager;

import java.util.List;

/**
 * Embedding Job Configuration
 * - 순차 처리 (병렬 없음)
 * - crawler_found = true, ready = false 조건으로 조회
 * - ORDER BY id ASC
 * - 키워드 9개씩 임베딩 생성 후 저장
 * - ready = true로 업데이트
 */
@Configuration
public class EmbeddingJobConfig {

    private static final Logger log = LoggerFactory.getLogger(EmbeddingJobConfig.class);

    private final EmbeddingClient embeddingClient;
    private final PlaceRepository placeRepository;
    private final PlaceKeywordEmbeddingRepository embeddingRepository;

    @Value("${batch.embedding.chunk-size:5}")
    private int chunkSize;

    public EmbeddingJobConfig(
            EmbeddingClient embeddingClient,
            PlaceRepository placeRepository,
            PlaceKeywordEmbeddingRepository embeddingRepository
    ) {
        this.embeddingClient = embeddingClient;
        this.placeRepository = placeRepository;
        this.embeddingRepository = embeddingRepository;
    }

    @Bean
    public Job embeddingJob(JobRepository jobRepository, Step embeddingStep) {
        return new JobBuilder("embeddingJob", jobRepository)
                .start(embeddingStep)
                .build();
    }

    @Bean
    public Step embeddingStep(
            JobRepository jobRepository,
            PlatformTransactionManager transactionManager,
            ItemReader<Place> embeddingReader,
            ItemProcessor<Place, Place> embeddingProcessor,
            ItemWriter<Place> embeddingWriter
    ) {
        log.info("Embedding step configured: chunkSize={} (sequential processing)", chunkSize);

        return new StepBuilder("embeddingStep", jobRepository)
                .<Place, Place>chunk(chunkSize, transactionManager)
                .reader(embeddingReader)
                .processor(embeddingProcessor)
                .writer(embeddingWriter)
                .faultTolerant()
                .skip(Exception.class)
                .skipLimit(Integer.MAX_VALUE)
                .noRollback(Exception.class)
                .build();
    }

    @Bean
    public ItemReader<Place> embeddingReader() {
        return new EmbeddingReader(placeRepository, 10);
    }

    @Bean
    public ItemProcessor<Place, Place> embeddingProcessor() {
        return place -> {
            try {
                // Check if place has keywords
                List<String> keywords = place.getKeyword();
                if (keywords == null || keywords.isEmpty()) {
                    log.warn("No keywords found for '{}' (ID: {}) - skipping", place.getName(), place.getId());
                    return null;
                }

                // Take only first 9 keywords
                List<String> keywordsToProcess = keywords.size() > 9
                        ? keywords.subList(0, 9)
                        : keywords;

                log.info("Processing {} keywords for '{}' (ID: {}): {}",
                        keywordsToProcess.size(), place.getName(), place.getId(),
                        String.join(", ", keywordsToProcess));

                // Delete existing embeddings for this place
                embeddingRepository.deleteByPlaceId(place.getId());

                // Call embedding service
                EmbeddingResponse response = embeddingClient.getEmbeddings(keywordsToProcess);

                if (!response.hasValidEmbeddings()) {
                    log.warn("No valid embeddings returned for '{}' (ID: {}) - skipping", place.getName(), place.getId());
                    return null;
                }

                List<float[]> embeddings = response.getEmbeddingsAsFloatArrays();
                log.info("Received {} embeddings for '{}' (ID: {})", embeddings.size(), place.getName(), place.getId());

                // Validate embeddings
                int validCount = 0;
                for (float[] embedding : embeddings) {
                    boolean isNonZero = false;
                    for (float v : embedding) {
                        if (v != 0.0f) {
                            isNonZero = true;
                            break;
                        }
                    }
                    if (isNonZero) validCount++;
                }

                if (validCount == 0) {
                    log.warn("All embeddings are zero vectors for '{}' (ID: {}) - skipping", place.getName(), place.getId());
                    return null;
                }

                // Save embeddings
                int savedCount = 0;
                for (int i = 0; i < Math.min(keywordsToProcess.size(), embeddings.size()); i++) {
                    String keyword = keywordsToProcess.get(i);
                    float[] embedding = embeddings.get(i);

                    PlaceKeywordEmbedding embeddingEntity = new PlaceKeywordEmbedding(
                            place.getId(),
                            keyword,
                            embedding
                    );

                    embeddingRepository.save(embeddingEntity);
                    savedCount++;
                }

                log.info("Saved {} embeddings for '{}' (ID: {})", savedCount, place.getName(), place.getId());

                // Mark place as embed_status=COMPLETED
                place.setEmbedStatus(EmbedStatus.COMPLETED);

                log.info("Successfully embedded '{}' (ID: {}) - Keywords: {}, embed_status=COMPLETED",
                        place.getName(), place.getId(), String.join(", ", keywordsToProcess));

                return place;

            } catch (EmbeddingClient.EmbeddingServiceException e) {
                log.error("Embedding service error for '{}' (ID: {}): {}",
                        place.getName(), place.getId(), e.getMessage());
                return null;

            } catch (Exception e) {
                log.error("Embedding failed for '{}' (ID: {}): {}",
                        place.getName(), place.getId(), e.getMessage());
                return null;
            }
        };
    }

    @Bean
    public ItemWriter<Place> embeddingWriter() {
        return chunk -> {
            log.info("Saving {} places with ready=true...", chunk.getItems().size());

            for (Place place : chunk.getItems()) {
                try {
                    placeRepository.saveAndFlush(place);
                    log.debug("Updated place '{}' (ID: {}) with ready=true", place.getName(), place.getId());
                } catch (Exception e) {
                    log.error("Failed to save place ID {}: {}", place.getId(), e.getMessage());
                }
            }

            log.info("Successfully updated {} places", chunk.getItems().size());
        };
    }
}
