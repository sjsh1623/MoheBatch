package com.example.ingestion.batch.processor;

import com.example.ingestion.batch.reader.EnrichedPlace;
import com.example.ingestion.dto.ProcessedPlaceJava;
import com.example.ingestion.service.PlaceEnrichmentService;
import com.example.ingestion.service.PlaceFilterService;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Simplified place enrichment processor for Java-only batch
 */
@Component
public class SimplifiedPlaceEnrichmentProcessor implements ItemProcessor<EnrichedPlace, ProcessedPlaceJava> {

    private static final Logger logger = LoggerFactory.getLogger(SimplifiedPlaceEnrichmentProcessor.class);

    private final PlaceEnrichmentService enrichmentService;
    private final PlaceFilterService filterService;
    private final MeterRegistry meterRegistry;

    // Performance monitoring
    private final AtomicLong processedCount = new AtomicLong(0);
    private final AtomicLong filteredCount = new AtomicLong(0);
    private final AtomicLong errorCount = new AtomicLong(0);

    // Configuration
    private final int maxRetries;
    private final Duration apiTimeout;
    private final boolean enableStrictFiltering;

    public SimplifiedPlaceEnrichmentProcessor(
            PlaceEnrichmentService enrichmentService,
            PlaceFilterService filterService,
            MeterRegistry meterRegistry,
            @Value("${app.batch.max-retries:2}") int maxRetries,
            @Value("${app.batch.api-timeout:30s}") Duration apiTimeout,
            @Value("${app.batch.strict-filtering:true}") boolean enableStrictFiltering
    ) {
        this.enrichmentService = enrichmentService;
        this.filterService = filterService;
        this.meterRegistry = meterRegistry;
        this.maxRetries = maxRetries;
        this.apiTimeout = apiTimeout;
        this.enableStrictFiltering = enableStrictFiltering;
    }

    @Override
    public ProcessedPlaceJava process(EnrichedPlace item) {
        try {
            long startTime = System.currentTimeMillis();

            // Validate input
            if (!isValidPlace(item)) {
                meterRegistry.counter("place_processor_invalid_input").increment();
                return null;
            }

            // Create basic processed place
            ProcessedPlaceJava processedPlace = createBasicProcessedPlace(item);

            // Early filtering check
            if (enableStrictFiltering && filterService.shouldExcludePlace(processedPlace)) {
                logger.debug("Filtered out place early: {}", item.getNaverPlace().getCleanTitle());
                filteredCount.incrementAndGet();
                meterRegistry.counter("place_processor_filtered_early").increment();
                return null;
            }

            // Enhance place with LLM services
            ProcessedPlaceJava enrichedPlace = enhancePlaceAsync(item, processedPlace)
                    .timeout(apiTimeout)
                    .doOnError(error -> {
                        logger.warn("Failed to enrich place '{}': {}",
                                   item.getNaverPlace().getCleanTitle(), error.getMessage());
                        errorCount.incrementAndGet();
                        meterRegistry.counter("place_processor_error").increment();
                    })
                    .onErrorReturn(processedPlace)
                    .block();

            // Final filtering check
            if (enableStrictFiltering && enrichedPlace != null &&
                filterService.shouldExcludePlace(enrichedPlace)) {
                logger.debug("Filtered out place after enrichment: {}",
                           item.getNaverPlace().getCleanTitle());
                filteredCount.incrementAndGet();
                meterRegistry.counter("place_processor_filtered_final").increment();
                return null;
            }

            if (enrichedPlace != null) {
                processedCount.incrementAndGet();
                long processingTime = System.currentTimeMillis() - startTime;
                meterRegistry.timer("place_processor_duration").record(Duration.ofMillis(processingTime));
                meterRegistry.counter("place_processor_success").increment();

                logger.debug("Successfully processed place: {} in {}ms",
                           item.getNaverPlace().getCleanTitle(), processingTime);
            }

            return enrichedPlace;

        } catch (Exception e) {
            logger.error("Unexpected error processing place '{}': {}",
                        item.getNaverPlace().getCleanTitle(), e.getMessage(), e);
            errorCount.incrementAndGet();
            meterRegistry.counter("place_processor_unexpected_error").increment();
            return null;
        }
    }

    /**
     * Enhance place with LLM services using async processing
     */
    private Mono<ProcessedPlaceJava> enhancePlaceAsync(EnrichedPlace item, ProcessedPlaceJava basicPlace) {
        String placeName = item.getNaverPlace().getCleanTitle();
        String contextInfo = buildContextInfo(item);

        // Build comprehensive prompt for description
        String descriptionPrompt = buildDescriptionPrompt(item, contextInfo);

        // Generate description
        Mono<String> descriptionMono = enrichmentService.generateDescription(descriptionPrompt)
                .subscribeOn(Schedulers.boundedElastic())
                .doOnNext(desc -> logger.debug("Generated description for {}: {} chars", placeName, desc.length()))
                .onErrorReturn("");

        return descriptionMono.flatMap(description -> {
            // Use generated description if available
            String finalDescription = !description.isEmpty() ? description : basicPlace.getDescription();
            basicPlace.setDescription(finalDescription);

            // Generate embedding from description
            String embeddingText = buildEmbeddingText(item, finalDescription);

            addRandomDelay(); // Rate limiting

            return enrichmentService.generateEmbedding(embeddingText)
                    .subscribeOn(Schedulers.boundedElastic())
                    .map(embedding -> {
                        basicPlace.setKeywordVector(embedding);

                        // Update source flags
                        basicPlace.getSourceFlags().put("hasAiDescription", !description.isEmpty());
                        basicPlace.getSourceFlags().put("hasEmbedding", !embedding.isEmpty());
                        basicPlace.getSourceFlags().put("processingTimestamp", System.currentTimeMillis());

                        meterRegistry.counter("place_processor_enriched").increment();
                        return basicPlace;
                    })
                    .onErrorReturn(basicPlace);
        });
    }

    /**
     * Validate if place has minimum required data
     */
    private boolean isValidPlace(EnrichedPlace item) {
        if (item == null || item.getNaverPlace() == null) {
            return false;
        }

        String title = item.getNaverPlace().getCleanTitle();
        if (title == null || title.trim().isEmpty()) {
            logger.debug("Skipping place with empty title");
            return false;
        }

        Double latitude = item.getNaverPlace().getLatitude();
        Double longitude = item.getNaverPlace().getLongitude();
        if (latitude == null || longitude == null || latitude == 0.0 || longitude == 0.0) {
            logger.debug("Skipping place '{}' with invalid coordinates", title);
            return false;
        }

        return true;
    }

    /**
     * Create basic processed place without LLM enhancements
     */
    private ProcessedPlaceJava createBasicProcessedPlace(EnrichedPlace item) {
        ProcessedPlaceJava place = new ProcessedPlaceJava();

        // Basic information
        place.setNaverPlaceId(generateNaverPlaceId(item.getNaverPlace()));
        place.setGooglePlaceId(item.getGooglePlace() != null ? item.getGooglePlace().getPlaceId() : null);
        place.setName(item.getNaverPlace().getCleanTitle());
        place.setCategory(item.getNaverPlace().getCategory());
        place.setAddress(item.getNaverPlace().getAddress());
        place.setRoadAddress(item.getNaverPlace().getRoadAddress());

        // Coordinates
        place.setLatitude(new BigDecimal(item.getNaverPlace().getLatitude().toString()));
        place.setLongitude(new BigDecimal(item.getNaverPlace().getLongitude().toString()));

        // Google Place data if available
        if (item.getGooglePlace() != null) {
            place.setPhone(item.getGooglePlace().getFormattedPhoneNumber());
            place.setWebsiteUrl(item.getGooglePlace().getWebsite());
            place.setRating(item.getGooglePlace().getRating());
            place.setUserRatingsTotal(item.getGooglePlace().getUserRatingsTotal());
            place.setPriceLevel(item.getGooglePlace().getPriceLevel());
            place.setTypes(item.getGooglePlace().getTypes());
        }

        // Initialize description with Naver data as fallback
        place.setDescription(item.getNaverPlace().getDescription() != null ?
                           item.getNaverPlace().getDescription() : "");

        // Source flags
        place.getSourceFlags().put("hasNaverData", true);
        place.getSourceFlags().put("hasGoogleData", item.getGooglePlace() != null);

        return place;
    }

    /**
     * Build comprehensive description prompt
     */
    private String buildDescriptionPrompt(EnrichedPlace item, String contextInfo) {
        String placeName = item.getNaverPlace().getCleanTitle();
        String category = item.getNaverPlace().getCategory();
        String originalDesc = item.getNaverPlace().getDescription();

        return String.format("""
                이 장소를 직접 방문해서 느낀 생생한 체험담을 친근한 존댓말로 작성해 주세요.

                장소 정보:
                - 이름: %s
                - 카테고리: %s
                - 기존 설명: %s
                - 추가 정보: %s

                작성 가이드:
                1. 500-800자 분량으로 이 장소만의 특별한 매력을 진심이 담긴 존댓말로 작성
                2. 반드시 3문장 이상의 완전한 문장으로 구성하여 자연스럽게 읽히도록 작성
                3. "여기 정말 좋아요", "추천드려요", "기분이 좋아져요" 같은 친근하고 공감 가는 존댓말 사용
                4. 실제 방문객의 솔직한 후기처럼 개인적인 감정과 경험을 생생하게 표현
                5. 절대 금지: 주소, 위치 정보, 전화번호, 운영시간, 가격 정보 언급 금지

                마치 친한 친구에게 추천하는 듯한 따뜻하고 정감 있는 후기:
                """, placeName, category, originalDesc, contextInfo);
    }

    /**
     * Build context information for description generation
     */
    private String buildContextInfo(EnrichedPlace item) {
        StringBuilder context = new StringBuilder();

        if (item.getGooglePlace() != null) {
            if (item.getGooglePlace().getRating() != null) {
                context.append("평점 ").append(item.getGooglePlace().getRating()).append("/5.0");
                if (item.getGooglePlace().getUserRatingsTotal() != null) {
                    context.append(" (").append(item.getGooglePlace().getUserRatingsTotal()).append("개 리뷰)");
                }
            }
        }

        return context.toString();
    }

    /**
     * Build text for embedding generation
     */
    private String buildEmbeddingText(EnrichedPlace item, String description) {
        StringBuilder text = new StringBuilder();

        text.append(item.getNaverPlace().getCleanTitle()).append(" ");
        text.append(item.getNaverPlace().getCategory()).append(" ");
        text.append(description).append(" ");
        text.append(item.getNaverPlace().getAddress());

        return text.toString().trim();
    }

    /**
     * Generate unique Naver place ID
     */
    private String generateNaverPlaceId(com.example.ingestion.dto.NaverPlaceItem naverPlace) {
        String identifier = String.format("%s-%s-%s-%s",
                naverPlace.getCleanTitle(),
                naverPlace.getAddress(),
                naverPlace.getLatitude(),
                naverPlace.getLongitude());

        return java.util.Base64.getEncoder()
                .encodeToString(identifier.getBytes())
                .substring(0, Math.min(20, identifier.length()));
    }

    /**
     * Add random delay to prevent API rate limiting
     */
    private void addRandomDelay() {
        try {
            int delayMs = ThreadLocalRandom.current().nextInt(1000, 3000); // 1-3 seconds
            Thread.sleep(delayMs);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.warn("Delay interrupted");
        }
    }

    /**
     * Get processing statistics
     */
    public ProcessingStats getProcessingStats() {
        return new ProcessingStats(
                processedCount.get(),
                filteredCount.get(),
                errorCount.get()
        );
    }

    public static class ProcessingStats {
        private final long processed;
        private final long filtered;
        private final long errors;

        public ProcessingStats(long processed, long filtered, long errors) {
            this.processed = processed;
            this.filtered = filtered;
            this.errors = errors;
        }

        public long getProcessed() { return processed; }
        public long getFiltered() { return filtered; }
        public long getErrors() { return errors; }

        @Override
        public String toString() {
            return String.format("ProcessingStats{processed=%d, filtered=%d, errors=%d}",
                               processed, filtered, errors);
        }
    }
}