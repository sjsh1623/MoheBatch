package com.mohe.batch.job;

import com.mohe.batch.dto.embedding.EmbeddingResponse;
import com.mohe.batch.entity.EmbedStatus;
import com.mohe.batch.entity.Place;
import com.mohe.batch.entity.PlaceKeywordEmbedding;
import com.mohe.batch.entity.PlaceMenu;
import com.mohe.batch.entity.PlaceMenuEmbedding;
import com.mohe.batch.repository.PlaceKeywordEmbeddingRepository;
import com.mohe.batch.repository.PlaceMenuEmbeddingRepository;
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
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.PlatformTransactionManager;

import java.util.List;

/**
 * All Embedding Job Configuration
 * - 키워드 + 메뉴 모두 임베딩
 * - 순차 처리 (병렬 없음)
 * - crawl_status = COMPLETED 조건
 */
@Configuration
public class AllEmbeddingJobConfig {

    private static final Logger log = LoggerFactory.getLogger(AllEmbeddingJobConfig.class);
    private static final int MAX_KEYWORDS = 9;
    private static final int MAX_MENUS = 20;

    private final EmbeddingClient embeddingClient;
    private final PlaceRepository placeRepository;
    private final PlaceKeywordEmbeddingRepository keywordEmbeddingRepository;
    private final PlaceMenuEmbeddingRepository menuEmbeddingRepository;

    @Value("${batch.embedding.chunk-size:5}")
    private int chunkSize;

    public AllEmbeddingJobConfig(
            EmbeddingClient embeddingClient,
            PlaceRepository placeRepository,
            PlaceKeywordEmbeddingRepository keywordEmbeddingRepository,
            PlaceMenuEmbeddingRepository menuEmbeddingRepository
    ) {
        this.embeddingClient = embeddingClient;
        this.placeRepository = placeRepository;
        this.keywordEmbeddingRepository = keywordEmbeddingRepository;
        this.menuEmbeddingRepository = menuEmbeddingRepository;
    }

    @Bean
    public Job allEmbeddingJob(JobRepository jobRepository, @Qualifier("allEmbeddingStep") Step allEmbeddingStep) {
        return new JobBuilder("allEmbeddingJob", jobRepository)
                .start(allEmbeddingStep)
                .build();
    }

    @Bean
    public Step allEmbeddingStep(
            JobRepository jobRepository,
            PlatformTransactionManager transactionManager,
            @Qualifier("allEmbeddingReader") ItemReader<Place> allEmbeddingReader,
            @Qualifier("allEmbeddingProcessor") ItemProcessor<Place, Place> allEmbeddingProcessor,
            @Qualifier("allEmbeddingWriter") ItemWriter<Place> allEmbeddingWriter
    ) {
        log.info("All embedding step configured: chunkSize={} (sequential processing)", chunkSize);

        return new StepBuilder("allEmbeddingStep", jobRepository)
                .<Place, Place>chunk(chunkSize, transactionManager)
                .reader(allEmbeddingReader)
                .processor(allEmbeddingProcessor)
                .writer(allEmbeddingWriter)
                .faultTolerant()
                .skip(Exception.class)
                .skipLimit(Integer.MAX_VALUE)
                .noRollback(Exception.class)
                .build();
    }

    @Bean
    public ItemReader<Place> allEmbeddingReader() {
        return new AllEmbeddingReader(placeRepository, 10);
    }

    @Bean
    public ItemProcessor<Place, Place> allEmbeddingProcessor() {
        return place -> {
            try {
                boolean keywordProcessed = processKeywords(place);
                boolean menuProcessed = processMenus(place);

                if (!keywordProcessed && !menuProcessed) {
                    log.warn("No keywords or menus processed for '{}' (ID: {})", place.getName(), place.getId());
                    return null;
                }

                return place;

            } catch (Exception e) {
                log.error("All embedding failed for '{}' (ID: {}): {}",
                        place.getName(), place.getId(), e.getMessage());
                return null;
            }
        };
    }

    private boolean processKeywords(Place place) {
        // Skip if already completed
        if (place.getEmbedStatus() == EmbedStatus.COMPLETED) {
            log.debug("Keywords already embedded for '{}' (ID: {})", place.getName(), place.getId());
            return true;
        }

        List<String> keywords = place.getKeyword();
        if (keywords == null || keywords.isEmpty()) {
            log.debug("No keywords found for '{}' (ID: {})", place.getName(), place.getId());
            place.setEmbedStatus(EmbedStatus.COMPLETED);
            return true;
        }

        List<String> keywordsToProcess = keywords.size() > MAX_KEYWORDS
                ? keywords.subList(0, MAX_KEYWORDS)
                : keywords;

        log.info("Processing {} keywords for '{}' (ID: {})",
                keywordsToProcess.size(), place.getName(), place.getId());

        try {
            // Delete existing embeddings
            keywordEmbeddingRepository.deleteByPlaceId(place.getId());

            // Call embedding service
            EmbeddingResponse response = embeddingClient.getEmbeddings(keywordsToProcess);

            if (!response.hasValidEmbeddings()) {
                log.warn("No valid keyword embeddings returned for '{}' (ID: {})", place.getName(), place.getId());
                return false;
            }

            List<float[]> embeddings = response.getEmbeddingsAsFloatArrays();

            // Save embeddings
            int savedCount = 0;
            for (int i = 0; i < Math.min(keywordsToProcess.size(), embeddings.size()); i++) {
                String keyword = keywordsToProcess.get(i);
                float[] embedding = embeddings.get(i);

                if (isZeroVector(embedding)) continue;

                PlaceKeywordEmbedding embeddingEntity = new PlaceKeywordEmbedding(
                        place.getId(),
                        keyword,
                        embedding
                );

                keywordEmbeddingRepository.save(embeddingEntity);
                savedCount++;
            }

            log.info("Saved {} keyword embeddings for '{}' (ID: {})", savedCount, place.getName(), place.getId());
            place.setEmbedStatus(EmbedStatus.COMPLETED);
            return true;

        } catch (Exception e) {
            log.error("Keyword embedding failed for '{}' (ID: {}): {}", place.getName(), place.getId(), e.getMessage());
            return false;
        }
    }

    private boolean processMenus(Place place) {
        // Skip if already completed
        if (place.getMenuEmbedStatus() == EmbedStatus.COMPLETED) {
            log.debug("Menus already embedded for '{}' (ID: {})", place.getName(), place.getId());
            return true;
        }

        List<PlaceMenu> menus = place.getMenus();
        if (menus == null || menus.isEmpty()) {
            log.debug("No menus found for '{}' (ID: {})", place.getName(), place.getId());
            place.setMenuEmbedStatus(EmbedStatus.COMPLETED);
            return true;
        }

        List<PlaceMenu> menusToProcess = menus.size() > MAX_MENUS
                ? menus.subList(0, MAX_MENUS)
                : menus;

        List<String> menuNames = menusToProcess.stream()
                .map(PlaceMenu::getName)
                .filter(name -> name != null && !name.isBlank())
                .toList();

        if (menuNames.isEmpty()) {
            log.debug("No valid menu names found for '{}' (ID: {})", place.getName(), place.getId());
            place.setMenuEmbedStatus(EmbedStatus.COMPLETED);
            return true;
        }

        log.info("Processing {} menus for '{}' (ID: {})",
                menuNames.size(), place.getName(), place.getId());

        try {
            // Delete existing menu embeddings
            menuEmbeddingRepository.deleteByPlaceId(place.getId());

            // Call embedding service
            EmbeddingResponse response = embeddingClient.getEmbeddings(menuNames);

            if (!response.hasValidEmbeddings()) {
                log.warn("No valid menu embeddings returned for '{}' (ID: {})", place.getName(), place.getId());
                return false;
            }

            List<float[]> embeddings = response.getEmbeddingsAsFloatArrays();

            // Save embeddings
            int savedCount = 0;
            for (int i = 0; i < Math.min(menusToProcess.size(), embeddings.size()); i++) {
                PlaceMenu menu = menusToProcess.get(i);
                float[] embedding = embeddings.get(i);

                if (isZeroVector(embedding)) continue;

                PlaceMenuEmbedding embeddingEntity = new PlaceMenuEmbedding(
                        place.getId(),
                        menu.getId(),
                        menu.getName(),
                        embedding
                );

                menuEmbeddingRepository.save(embeddingEntity);
                savedCount++;
            }

            log.info("Saved {} menu embeddings for '{}' (ID: {})", savedCount, place.getName(), place.getId());
            place.setMenuEmbedStatus(EmbedStatus.COMPLETED);
            return true;

        } catch (Exception e) {
            log.error("Menu embedding failed for '{}' (ID: {}): {}", place.getName(), place.getId(), e.getMessage());
            return false;
        }
    }

    private boolean isZeroVector(float[] vector) {
        for (float v : vector) {
            if (v != 0.0f) return false;
        }
        return true;
    }

    @Bean
    public ItemWriter<Place> allEmbeddingWriter() {
        return chunk -> {
            log.info("Saving {} places with embed_status and menu_embed_status...", chunk.getItems().size());

            for (Place place : chunk.getItems()) {
                try {
                    placeRepository.saveAndFlush(place);
                    log.debug("Updated place '{}' (ID: {}) - embed_status={}, menu_embed_status={}",
                            place.getName(), place.getId(),
                            place.getEmbedStatus(), place.getMenuEmbedStatus());
                } catch (Exception e) {
                    log.error("Failed to save place ID {}: {}", place.getId(), e.getMessage());
                }
            }

            log.info("Successfully updated {} places", chunk.getItems().size());
        };
    }
}
