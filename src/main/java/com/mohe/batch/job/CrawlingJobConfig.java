package com.mohe.batch.job;

import com.mohe.batch.dto.crawling.CrawledDataDto;
import com.mohe.batch.dto.crawling.MenuDataDto;
import com.mohe.batch.dto.crawling.MenuItemDto;
import com.mohe.batch.dto.crawling.WeeklyHoursDto;
import com.mohe.batch.entity.*;
import com.mohe.batch.repository.PlaceRepository;
import com.mohe.batch.service.CrawlingService;
import com.mohe.batch.service.ImageProcessorClient;
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
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.TaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;

import java.time.LocalTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;

@Configuration
public class CrawlingJobConfig {

    private static final Logger log = LoggerFactory.getLogger(CrawlingJobConfig.class);

    private final CrawlingService crawlingService;
    private final OpenAiDescriptionService openAiDescriptionService;
    private final ImageProcessorClient imageProcessorClient;
    private final PlaceRepository placeRepository;

    @Value("${batch.async.core-pool-size:5}")
    private int corePoolSize;

    @Value("${batch.async.max-pool-size:10}")
    private int maxPoolSize;

    @Value("${batch.async.queue-capacity:100}")
    private int queueCapacity;

    @Value("${batch.chunk-size:10}")
    private int chunkSize;

    @Value("${batch.worker.total-workers:3}")
    private int totalWorkers;

    public CrawlingJobConfig(
            CrawlingService crawlingService,
            OpenAiDescriptionService openAiDescriptionService,
            ImageProcessorClient imageProcessorClient,
            PlaceRepository placeRepository
    ) {
        this.crawlingService = crawlingService;
        this.openAiDescriptionService = openAiDescriptionService;
        this.imageProcessorClient = imageProcessorClient;
        this.placeRepository = placeRepository;
    }

    @Bean(name = "batchTaskExecutor")
    public TaskExecutor batchTaskExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(corePoolSize);
        executor.setMaxPoolSize(maxPoolSize);
        executor.setQueueCapacity(queueCapacity);
        executor.setThreadNamePrefix("batch-async-");
        executor.setWaitForTasksToCompleteOnShutdown(true);
        executor.setAwaitTerminationSeconds(60);
        executor.initialize();

        log.info("Batch TaskExecutor initialized: core={}, max={}, queue={}",
                corePoolSize, maxPoolSize, queueCapacity);
        return executor;
    }

    @Bean
    public Job crawlingJob(JobRepository jobRepository, Step crawlingStep) {
        return new JobBuilder("crawlingJob", jobRepository)
                .start(crawlingStep)
                .build();
    }

    @Bean
    public Step crawlingStep(
            JobRepository jobRepository,
            PlatformTransactionManager transactionManager,
            ItemReader<Place> crawlingReader,
            ItemProcessor<Place, Place> crawlingProcessor,
            ItemWriter<Place> crawlingWriter,
            TaskExecutor batchTaskExecutor
    ) {
        // AsyncItemProcessor 설정
        AsyncItemProcessor<Place, Place> asyncItemProcessor = new AsyncItemProcessor<>();
        asyncItemProcessor.setDelegate(crawlingProcessor);
        asyncItemProcessor.setTaskExecutor(batchTaskExecutor);

        // AsyncItemWriter 설정
        AsyncItemWriter<Place> asyncItemWriter = new AsyncItemWriter<>();
        asyncItemWriter.setDelegate(crawlingWriter);

        try {
            asyncItemProcessor.afterPropertiesSet();
            asyncItemWriter.afterPropertiesSet();
        } catch (Exception e) {
            throw new RuntimeException("Failed to initialize async processors", e);
        }

        log.info("Async batch step configured: chunkSize={}", chunkSize);

        return new StepBuilder("crawlingStep", jobRepository)
                .<Place, Future<Place>>chunk(chunkSize, transactionManager)
                .reader(crawlingReader)
                .processor(asyncItemProcessor)
                .writer(asyncItemWriter)
                .faultTolerant()
                .skip(Exception.class)
                .skipLimit(Integer.MAX_VALUE)
                .noRollback(Exception.class)
                .build();
    }

    @Bean
    @StepScope
    public ItemReader<Place> crawlingReader(
            @Value("#{jobParameters['workerId']}") Long workerId
    ) {
        int workerIdInt = workerId != null ? workerId.intValue() : 0;
        log.info("Creating CrawlingReader for worker {}", workerIdInt);
        return new CrawlingReader(placeRepository, workerIdInt, totalWorkers, 10);
    }

    @Bean
    public ItemProcessor<Place, Place> crawlingProcessor() {
        return place -> {
            try {
                String searchQuery = place.getRoadAddress();
                if (!place.getDescriptions().isEmpty()) {
                    String savedQuery = place.getDescriptions().get(0).getSearchQuery();
                    if (savedQuery != null && !savedQuery.isEmpty()) {
                        searchQuery = savedQuery;
                    }
                }

                log.info("🚀 ========== 크롤링 시작 ========== '{}' (ID: {})", place.getName(), place.getId());
                var response = crawlingService.crawlPlaceData(searchQuery, place.getName()).block();

                if (response == null || response.getData() == null) {
                    log.error("❌ 크롤링 실패 '{}' - 응답 없음", place.getName());
                    place.setCrawlerFound(false);
                    place.setReady(false);
                    return place;
                }

                CrawledDataDto crawledData = response.getData();
                log.info("📥 응답 수신 '{}'", place.getName());

                // 리뷰 카운트 업데이트
                try {
                    place.setReviewCount(Integer.parseInt(crawledData.getReviewCount()));
                } catch (NumberFormatException e) {
                    place.setReviewCount(0);
                }
                place.setParkingAvailable(crawledData.isParkingAvailable());
                place.setPetFriendly(crawledData.isPetFriendly());

                // PlaceDescription 생성
                place.getDescriptions().clear();
                PlaceDescription description = new PlaceDescription();
                description.setPlace(place);
                description.setOriginalDescription(sanitizeText(crawledData.getOriginalDescription()));

                String aiSummaryText = "";
                if (crawledData.getAiSummary() != null && !crawledData.getAiSummary().isEmpty()) {
                    aiSummaryText = String.join("\n", crawledData.getAiSummary());
                }

                // Fallback 텍스트
                String textForKeywords;
                if (aiSummaryText != null && !aiSummaryText.trim().isEmpty()) {
                    textForKeywords = aiSummaryText;
                } else if (crawledData.getOriginalDescription() != null && !crawledData.getOriginalDescription().trim().isEmpty()) {
                    textForKeywords = crawledData.getOriginalDescription();
                } else if (crawledData.getReviews() != null && !crawledData.getReviews().isEmpty()) {
                    int reviewCount = Math.min(crawledData.getReviews().size(), 3);
                    textForKeywords = String.join("\n", crawledData.getReviews().subList(0, reviewCount));
                } else {
                    textForKeywords = null;
                }

                if (textForKeywords == null || textForKeywords.trim().isEmpty()) {
                    log.warn("Lack of information for '{}' - no description text", place.getName());
                    place.setCrawlerFound(true);
                    place.setReady(false);
                    return place;
                }

                description.setAiSummary(sanitizeText(aiSummaryText));
                description.setSearchQuery(sanitizeText(searchQuery));

                // OpenAI 설명 생성
                String categoryStr = place.getCategory() != null ? String.join(",", place.getCategory()) : "";
                String reviewsForPrompt = prepareReviewSnippet(crawledData.getReviews());

                OpenAiDescriptionService.DescriptionPayload payload =
                        new OpenAiDescriptionService.DescriptionPayload(
                                aiSummaryText,
                                reviewsForPrompt,
                                crawledData.getOriginalDescription(),
                                categoryStr,
                                place.getPetFriendly() != null && place.getPetFriendly()
                        );

                OpenAiDescriptionService.DescriptionResult descriptionResult =
                        openAiDescriptionService.generateDescription(payload).orElse(null);

                String moheDescription = descriptionResult != null ? descriptionResult.description() : null;
                List<String> keywords = descriptionResult != null ? descriptionResult.keywords() : List.of();

                // Fallback 설명
                if (moheDescription == null || moheDescription.trim().isEmpty()) {
                    log.warn("OpenAI generation failed for '{}', using fallback", place.getName());
                    String fallbackDescription = textForKeywords;
                    if (fallbackDescription.length() > 150) {
                        int lastPeriod = Math.max(fallbackDescription.substring(0, 150).lastIndexOf('.'),
                                fallbackDescription.substring(0, 150).lastIndexOf('!'));
                        lastPeriod = Math.max(lastPeriod, fallbackDescription.substring(0, 150).lastIndexOf('?'));
                        if (lastPeriod > 50) {
                            fallbackDescription = fallbackDescription.substring(0, lastPeriod + 1).trim();
                        } else {
                            fallbackDescription = fallbackDescription.substring(0, 147).trim() + "...";
                        }
                    }
                    moheDescription = fallbackDescription;
                }

                if (moheDescription == null || moheDescription.trim().isEmpty()) {
                    moheDescription = place.getName() + "에 대한 정보입니다.";
                }

                description.setMoheDescription(sanitizeText(moheDescription));
                place.getDescriptions().add(description);

                // 키워드 Fallback
                if (keywords.isEmpty() || keywords.size() != 9) {
                    List<String> fallbackKeywords = new ArrayList<>();
                    if (place.getCategory() != null && !place.getCategory().isEmpty()) {
                        fallbackKeywords.addAll(place.getCategory());
                    }
                    while (fallbackKeywords.size() < 9) {
                        fallbackKeywords.add("장소");
                    }
                    keywords = fallbackKeywords.subList(0, 9);
                }
                place.setKeyword(keywords);

                // 기존 이미지 파일 삭제 후 새 이미지 저장
                int deletedPlaceImages = imageProcessorClient.deletePlaceImages(place.getId());
                if (deletedPlaceImages > 0) {
                    log.info("🗑️ 기존 장소 이미지 {}개 삭제 (ID: {})", deletedPlaceImages, place.getId());
                }
                place.getImages().clear();
                if (crawledData.getImageUrls() != null && !crawledData.getImageUrls().isEmpty()) {
                    List<String> savedImagePaths = imageProcessorClient.savePlaceImages(
                            place.getId(),
                            place.getName(),
                            crawledData.getImageUrls()
                    );

                    for (int i = 0; i < savedImagePaths.size(); i++) {
                        PlaceImage placeImage = new PlaceImage();
                        placeImage.setPlace(place);
                        placeImage.setUrl(savedImagePaths.get(i));
                        placeImage.setOrderIndex(i + 1);
                        place.getImages().add(placeImage);
                    }
                }

                // 영업시간
                place.getBusinessHours().clear();
                if (crawledData.getBusinessHours() != null && crawledData.getBusinessHours().getWeekly() != null) {
                    for (Map.Entry<String, WeeklyHoursDto> entry : crawledData.getBusinessHours().getWeekly().entrySet()) {
                        PlaceBusinessHour businessHour = new PlaceBusinessHour();
                        businessHour.setPlace(place);
                        businessHour.setDayOfWeek(entry.getKey());

                        try {
                            if (entry.getValue().getOpen() != null && !entry.getValue().getOpen().isEmpty()) {
                                businessHour.setOpen(LocalTime.parse(entry.getValue().getOpen()));
                            }
                            if (entry.getValue().getClose() != null && !entry.getValue().getClose().isEmpty()) {
                                businessHour.setClose(LocalTime.parse(entry.getValue().getClose()));
                            }
                        } catch (Exception e) {
                            log.warn("Failed to parse business hours for {}: {}", place.getName(), e.getMessage());
                        }

                        businessHour.setDescription(sanitizeText(entry.getValue().getDescription()));
                        businessHour.setIsOperating(entry.getValue().isOperating());

                        if (crawledData.getBusinessHours().getLastOrderMinutes() != null) {
                            businessHour.setLastOrderMinutes(crawledData.getBusinessHours().getLastOrderMinutes());
                        }

                        place.getBusinessHours().add(businessHour);
                    }
                }

                // SNS
                place.getSns().clear();
                if (crawledData.getSnsUrls() != null && !crawledData.getSnsUrls().isEmpty()) {
                    for (Map.Entry<String, String> entry : crawledData.getSnsUrls().entrySet()) {
                        if (entry.getValue() != null && !entry.getValue().isEmpty()) {
                            PlaceSns sns = new PlaceSns();
                            sns.setPlace(place);
                            sns.setPlatform(entry.getKey());
                            sns.setUrl(entry.getValue());
                            place.getSns().add(sns);
                        }
                    }
                }

                // 리뷰
                place.getReviews().clear();
                int savedReviewCount = 0;
                if (crawledData.getReviews() != null && !crawledData.getReviews().isEmpty()) {
                    int reviewCount = Math.min(crawledData.getReviews().size(), 10);
                    for (int i = 0; i < reviewCount; i++) {
                        String reviewText = crawledData.getReviews().get(i);
                        if (reviewText != null && !reviewText.trim().isEmpty()) {
                            String sanitizedReviewText = sanitizeText(reviewText);
                            if (sanitizedReviewText != null && !sanitizedReviewText.trim().isEmpty()) {
                                PlaceReview review = new PlaceReview();
                                review.setPlace(place);
                                review.setReviewText(sanitizedReviewText);
                                review.setOrderIndex(i + 1);
                                place.getReviews().add(review);
                                savedReviewCount++;
                            }
                        }
                    }
                    log.info("📝 리뷰 {}개 저장 완료 '{}' (ID: {})", savedReviewCount, place.getName(), place.getId());
                } else {
                    log.debug("📭 리뷰 데이터 없음 '{}' (ID: {})", place.getName(), place.getId());
                }

                // 기존 메뉴 이미지 파일 삭제 후 메뉴 크롤링
                int deletedMenuImages = imageProcessorClient.deleteMenuImages(place.getId());
                if (deletedMenuImages > 0) {
                    log.info("🗑️ 기존 메뉴 이미지 {}개 삭제 (ID: {})", deletedMenuImages, place.getId());
                }
                place.getMenus().clear();
                try {
                    log.info("🍽️ 메뉴 크롤링 시작 '{}' (ID: {})", place.getName(), place.getId());
                    var menuResponse = crawlingService.crawlMenuData(searchQuery, place.getName()).block();

                    if (menuResponse != null && menuResponse.isSuccess() && menuResponse.getData() != null) {
                        MenuDataDto menuData = menuResponse.getData();
                        if (menuData.getMenus() != null && !menuData.getMenus().isEmpty()) {
                            int menuCount = 0;
                            int maxMenus = Math.min(menuData.getMenus().size(), 50); // 최대 50개 제한
                            int menuImageCount = 0;
                            for (int i = 0; i < maxMenus; i++) {
                                MenuItemDto menuItem = menuData.getMenus().get(i);
                                if (menuItem.getName() != null && !menuItem.getName().trim().isEmpty()) {
                                    PlaceMenu placeMenu = new PlaceMenu();
                                    placeMenu.setPlace(place);
                                    placeMenu.setName(sanitizeText(menuItem.getName()));
                                    placeMenu.setPrice(sanitizeText(menuItem.getPrice()));
                                    placeMenu.setDescription(sanitizeText(menuItem.getDescription()));
                                    placeMenu.setImageUrl(menuItem.getImageUrl());
                                    placeMenu.setDisplayOrder(i + 1);

                                    // 메뉴 이미지 저장 (ImageProcessor 서버 사용)
                                    if (menuItem.getImageUrl() != null && !menuItem.getImageUrl().isEmpty()) {
                                        try {
                                            String menuImagePath = imageProcessorClient.saveMenuImage(
                                                    place.getId(),
                                                    menuItem.getName(),
                                                    menuItem.getImageUrl()
                                            );
                                            placeMenu.setImagePath(menuImagePath);
                                            if (menuImagePath != null) menuImageCount++;
                                            log.debug("🖼️ 메뉴 이미지 저장 [{}/{}] {}", i + 1, maxMenus, menuItem.getName());
                                        } catch (Exception e) {
                                            log.warn("⚠️ 메뉴 이미지 저장 실패 '{}': {}", menuItem.getName(), e.getMessage());
                                        }
                                    }

                                    place.getMenus().add(placeMenu);
                                    menuCount++;
                                }
                            }
                            log.info("🍽️ 메뉴 {} / 이미지 {} 저장 완료 '{}'", menuCount, menuImageCount, place.getName());
                        }
                    } else {
                        log.debug("📭 메뉴 데이터 없음 '{}' (ID: {})", place.getName(), place.getId());
                    }
                } catch (Exception e) {
                    log.warn("⚠️ 메뉴 크롤링 실패 '{}': {}", place.getName(), e.getMessage());
                }

                place.setCrawlerFound(true);
                place.setReady(false);

                log.info("✅ ========== 크롤링 완료 ========== '{}' | 리뷰: {} | 이미지: {} | 메뉴: {}",
                        place.getName(), place.getReviewCount(), place.getImages().size(),
                        place.getMenus().size());

                return place;

            } catch (Exception e) {
                log.error("❌ 크롤링 실패 '{}': {}", place.getName(), e.getMessage());
                place.setCrawlerFound(false);
                place.setReady(false);
                return place;
            }
        };
    }

    @Bean
    public ItemWriter<Place> crawlingWriter() {
        return chunk -> {
            log.info("Saving batch of {} places...", chunk.getItems().size());
            int savedCount = 0;

            for (Place place : chunk.getItems()) {
                try {
                    // 컬렉션 데이터를 먼저 복사해둠 (JPA 세션 캐시로 인해 같은 객체를 참조할 수 있음)
                    List<PlaceDescription> newDescriptions = new ArrayList<>(place.getDescriptions());
                    List<PlaceImage> newImages = new ArrayList<>(place.getImages());
                    List<PlaceBusinessHour> newBusinessHours = new ArrayList<>(place.getBusinessHours());
                    List<PlaceSns> newSns = new ArrayList<>(place.getSns());
                    List<PlaceReview> newReviews = new ArrayList<>(place.getReviews());
                    List<PlaceMenu> newMenus = new ArrayList<>(place.getMenus());

                    // Fresh entity 조회
                    Place freshPlace = placeRepository.findById(place.getId())
                            .orElseThrow(() -> new IllegalStateException("Place not found: " + place.getId()));

                    // 기존 컬렉션 클리어 (orphanRemoval로 DB에서도 삭제)
                    freshPlace.getDescriptions().clear();
                    freshPlace.getImages().clear();
                    freshPlace.getBusinessHours().clear();
                    freshPlace.getSns().clear();
                    freshPlace.getReviews().clear();
                    freshPlace.getMenus().clear();

                    // orphans 삭제 실행
                    placeRepository.flush();

                    // 새 데이터 복사 (미리 복사해둔 데이터 사용)
                    updatePlaceFieldsWithCollections(freshPlace, place, newDescriptions, newImages,
                            newBusinessHours, newSns, newReviews, newMenus);

                    // 저장 (updated_at은 @PreUpdate로 자동 갱신)
                    placeRepository.saveAndFlush(freshPlace);
                    savedCount++;

                    log.info("Saved place '{}' (ID: {}, crawler_found={}, ready={})",
                            freshPlace.getName(), freshPlace.getId(),
                            freshPlace.getCrawlerFound(), freshPlace.getReady());

                } catch (Exception e) {
                    log.error("Failed to save place ID {}: {}", place.getId(), e.getMessage());
                }
            }

            log.info("Successfully saved {}/{} places", savedCount, chunk.getItems().size());
        };
    }

    private void updatePlaceFieldsWithCollections(
            Place target, Place source,
            List<PlaceDescription> descriptions,
            List<PlaceImage> images,
            List<PlaceBusinessHour> businessHours,
            List<PlaceSns> sns,
            List<PlaceReview> reviews,
            List<PlaceMenu> menus) {

        target.setName(source.getName());
        target.setLatitude(source.getLatitude());
        target.setLongitude(source.getLongitude());
        target.setRoadAddress(source.getRoadAddress());
        target.setWebsiteUrl(source.getWebsiteUrl());
        target.setRating(source.getRating());
        target.setReviewCount(source.getReviewCount());
        target.setCategory(source.getCategory());
        target.setKeyword(source.getKeyword());
        target.setParkingAvailable(source.getParkingAvailable());
        target.setPetFriendly(source.getPetFriendly());
        target.setReady(source.getReady());
        target.setCrawlerFound(source.getCrawlerFound());

        // 미리 복사해둔 컬렉션 데이터 사용
        if (descriptions != null) {
            descriptions.forEach(desc -> {
                desc.setPlace(target);
                target.getDescriptions().add(desc);
            });
        }
        if (images != null) {
            images.forEach(img -> {
                img.setPlace(target);
                target.getImages().add(img);
            });
        }
        if (businessHours != null) {
            businessHours.forEach(hour -> {
                hour.setPlace(target);
                target.getBusinessHours().add(hour);
            });
        }
        if (sns != null) {
            sns.forEach(s -> {
                s.setPlace(target);
                target.getSns().add(s);
            });
        }
        if (reviews != null) {
            reviews.forEach(review -> {
                review.setPlace(target);
                target.getReviews().add(review);
            });
        }
        if (menus != null) {
            menus.forEach(menu -> {
                menu.setPlace(target);
                target.getMenus().add(menu);
            });
        }
    }

    private String prepareReviewSnippet(List<String> reviews) {
        if (reviews == null || reviews.isEmpty()) {
            return "리뷰 정보 없음";
        }
        int limit = Math.min(reviews.size(), 10);
        return String.join("\n", reviews.subList(0, limit));
    }

    private String sanitizeText(String text) {
        if (text == null) {
            return null;
        }
        return text.replace("\u0000", "")
                .replace("\u0001", "")
                .replace("\u0002", "")
                .replace("\u0003", "")
                .replace("\u0004", "")
                .replace("\u0005", "")
                .replace("\u0006", "")
                .replace("\u0007", "")
                .replace("\u0008", "")
                .replace("\u000B", "")
                .replace("\u000C", "")
                .replace("\u000E", "")
                .replace("\u000F", "");
    }
}
