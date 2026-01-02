package com.mohe.batch.job;

import com.mohe.batch.dto.embedding.EmbeddingResponse;
import com.mohe.batch.entity.EmbedStatus;
import com.mohe.batch.entity.Place;
import com.mohe.batch.entity.PlaceMenu;
import com.mohe.batch.entity.PlaceMenuEmbedding;
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

import java.util.ArrayList;
import java.util.List;

/**
 * Menu Embedding Job Configuration
 * - 메뉴 이름을 임베딩
 * - 순차 처리 (병렬 없음)
 * - crawl_status = COMPLETED, menu_embed_status = PENDING 조건
 */
@Configuration
public class MenuEmbeddingJobConfig {

    private static final Logger log = LoggerFactory.getLogger(MenuEmbeddingJobConfig.class);
    private static final int MAX_MENUS_PER_BATCH = 20;

    private final EmbeddingClient embeddingClient;
    private final PlaceRepository placeRepository;
    private final PlaceMenuEmbeddingRepository menuEmbeddingRepository;

    @Value("${batch.embedding.chunk-size:5}")
    private int chunkSize;

    public MenuEmbeddingJobConfig(
            EmbeddingClient embeddingClient,
            PlaceRepository placeRepository,
            PlaceMenuEmbeddingRepository menuEmbeddingRepository
    ) {
        this.embeddingClient = embeddingClient;
        this.placeRepository = placeRepository;
        this.menuEmbeddingRepository = menuEmbeddingRepository;
    }

    @Bean
    public Job menuEmbeddingJob(JobRepository jobRepository, @Qualifier("menuEmbeddingStep") Step menuEmbeddingStep) {
        return new JobBuilder("menuEmbeddingJob", jobRepository)
                .start(menuEmbeddingStep)
                .build();
    }

    @Bean
    public Step menuEmbeddingStep(
            JobRepository jobRepository,
            PlatformTransactionManager transactionManager,
            @Qualifier("menuEmbeddingReader") ItemReader<Place> menuEmbeddingReader,
            @Qualifier("menuEmbeddingProcessor") ItemProcessor<Place, Place> menuEmbeddingProcessor,
            @Qualifier("menuEmbeddingWriter") ItemWriter<Place> menuEmbeddingWriter
    ) {
        log.info("Menu embedding step configured: chunkSize={} (sequential processing)", chunkSize);

        return new StepBuilder("menuEmbeddingStep", jobRepository)
                .<Place, Place>chunk(chunkSize, transactionManager)
                .reader(menuEmbeddingReader)
                .processor(menuEmbeddingProcessor)
                .writer(menuEmbeddingWriter)
                .faultTolerant()
                .skip(Exception.class)
                .skipLimit(Integer.MAX_VALUE)
                .noRollback(Exception.class)
                .build();
    }

    @Bean
    public ItemReader<Place> menuEmbeddingReader() {
        return new MenuEmbeddingReader(placeRepository, 10);
    }

    @Bean
    public ItemProcessor<Place, Place> menuEmbeddingProcessor() {
        return place -> {
            try {
                List<PlaceMenu> menus = place.getMenus();
                if (menus == null || menus.isEmpty()) {
                    log.warn("No menus found for '{}' (ID: {}) - skipping", place.getName(), place.getId());
                    // Mark as completed even if no menus (nothing to embed)
                    place.setMenuEmbedStatus(EmbedStatus.COMPLETED);
                    return place;
                }

                // Take only first MAX_MENUS_PER_BATCH menus
                List<PlaceMenu> menusToProcess = menus.size() > MAX_MENUS_PER_BATCH
                        ? menus.subList(0, MAX_MENUS_PER_BATCH)
                        : menus;

                List<String> menuNames = menusToProcess.stream()
                        .map(PlaceMenu::getName)
                        .filter(name -> name != null && !name.isBlank())
                        .toList();

                if (menuNames.isEmpty()) {
                    log.warn("No valid menu names found for '{}' (ID: {}) - skipping", place.getName(), place.getId());
                    place.setMenuEmbedStatus(EmbedStatus.COMPLETED);
                    return place;
                }

                log.info("Processing {} menus for '{}' (ID: {}): {}",
                        menuNames.size(), place.getName(), place.getId(),
                        String.join(", ", menuNames.subList(0, Math.min(5, menuNames.size()))) +
                        (menuNames.size() > 5 ? "..." : ""));

                // Delete existing menu embeddings for this place
                menuEmbeddingRepository.deleteByPlaceId(place.getId());

                // Call embedding service
                EmbeddingResponse response = embeddingClient.getEmbeddings(menuNames);

                if (!response.hasValidEmbeddings()) {
                    log.warn("No valid embeddings returned for '{}' (ID: {}) - skipping", place.getName(), place.getId());
                    return null;
                }

                List<float[]> embeddings = response.getEmbeddingsAsFloatArrays();
                log.info("Received {} embeddings for '{}' (ID: {})", embeddings.size(), place.getName(), place.getId());

                // Save embeddings
                int savedCount = 0;
                for (int i = 0; i < Math.min(menusToProcess.size(), embeddings.size()); i++) {
                    PlaceMenu menu = menusToProcess.get(i);
                    float[] embedding = embeddings.get(i);

                    // Validate non-zero embedding
                    boolean isNonZero = false;
                    for (float v : embedding) {
                        if (v != 0.0f) {
                            isNonZero = true;
                            break;
                        }
                    }
                    if (!isNonZero) continue;

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

                // Mark place as menu_embed_status=COMPLETED
                place.setMenuEmbedStatus(EmbedStatus.COMPLETED);

                log.info("Successfully embedded menus for '{}' (ID: {}) - {} menus, menu_embed_status=COMPLETED",
                        place.getName(), place.getId(), savedCount);

                return place;

            } catch (EmbeddingClient.EmbeddingServiceException e) {
                log.error("Embedding service error for '{}' (ID: {}): {}",
                        place.getName(), place.getId(), e.getMessage());
                return null;

            } catch (Exception e) {
                log.error("Menu embedding failed for '{}' (ID: {}): {}",
                        place.getName(), place.getId(), e.getMessage());
                return null;
            }
        };
    }

    @Bean
    public ItemWriter<Place> menuEmbeddingWriter() {
        return chunk -> {
            log.info("Saving {} places with menu_embed_status=COMPLETED...", chunk.getItems().size());

            for (Place place : chunk.getItems()) {
                try {
                    placeRepository.saveAndFlush(place);
                    log.debug("Updated place '{}' (ID: {}) with menu_embed_status=COMPLETED",
                            place.getName(), place.getId());
                } catch (Exception e) {
                    log.error("Failed to save place ID {}: {}", place.getId(), e.getMessage());
                }
            }

            log.info("Successfully updated {} places", chunk.getItems().size());
        };
    }
}
