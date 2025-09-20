package com.example.ingestion.service;

import com.example.ingestion.dto.GooglePlaceDetail;
import com.example.ingestion.dto.NaverPlaceItem;
import com.example.ingestion.entity.Place;
import com.example.ingestion.entity.PlaceImage;
import com.example.ingestion.repository.PlaceRepository;
import com.example.ingestion.repository.PlaceImageRepository;
import com.example.ingestion.service.impl.PlaceEnrichmentServiceImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

/**
 * 통합 장소 수집 서비스
 * 정부API → 네이버API → 구글API → OpenAI → Ollama → DB저장 전체 파이프라인
 */
@Service
public class IntegratedPlaceCollectionService {

    private static final Logger logger = LoggerFactory.getLogger(IntegratedPlaceCollectionService.class);

    private final GovernmentApiService governmentApiService;
    private final NaverApiService naverApiService;
    private final GooglePlacesApiService googlePlacesApiService;
    private final PlaceEnrichmentServiceImpl enrichmentService;
    private final PlaceFilterService filterService;
    private final ImageMappingService imageMappingService;
    private final PlaceRepository placeRepository;
    private final PlaceImageRepository placeImageRepository;

    public IntegratedPlaceCollectionService(
            GovernmentApiService governmentApiService,
            NaverApiService naverApiService,
            GooglePlacesApiService googlePlacesApiService,
            PlaceEnrichmentServiceImpl enrichmentService,
            PlaceFilterService filterService,
            ImageMappingService imageMappingService,
            PlaceRepository placeRepository,
            PlaceImageRepository placeImageRepository
    ) {
        this.governmentApiService = governmentApiService;
        this.naverApiService = naverApiService;
        this.googlePlacesApiService = googlePlacesApiService;
        this.enrichmentService = enrichmentService;
        this.filterService = filterService;
        this.imageMappingService = imageMappingService;
        this.placeRepository = placeRepository;
        this.placeImageRepository = placeImageRepository;
    }

    /**
     * 전체 배치 프로세스 실행
     * 1. 행정구역 수집 → 2. 네이버 장소 수집 → 3. 구글 상세정보 → 4. OpenAI 설명생성 → 5. DB저장
     */
    public Flux<CollectionResult> executeFullCollectionProcess() {
        logger.info("🚀 Starting full place collection process");

        return governmentApiService.getAllAdministrativeRegions()
                .take(50) // 테스트용으로 제한 (실제 배포시 제거)
                .delayElements(Duration.ofSeconds(1)) // API 부하 방지
                .flatMap(this::collectPlacesForRegion)
                .doOnNext(result -> logger.info("📊 Collection result: {} places processed for region {}",
                        result.getProcessedCount(), result.getRegionName()))
                .doFinally(signalType -> logger.info("🏁 Full collection process completed with signal: {}", signalType));
    }

    /**
     * 특정 지역의 장소들을 수집합니다
     */
    public Mono<CollectionResult> collectPlacesForRegion(GovernmentApiService.AdministrativeRegion region) {
        String regionName = region.getFullAddress();
        logger.info("🏘️ Collecting places for region: {}", regionName);

        return naverApiService.searchPlacesByRegion(regionName)
                .take(100) // 지역당 최대 100개 장소
                .delayElements(Duration.ofMillis(300)) // Rate limiting
                .flatMap(this::processNaverPlace)
                .collectList()
                .map(results -> {
                    long successCount = results.stream().mapToLong(ProcessResult::getProcessedCount).sum();
                    long failureCount = results.stream().mapToLong(ProcessResult::getFailedCount).sum();

                    return new CollectionResult(
                            regionName,
                            (int) successCount,
                            (int) failureCount,
                            ZonedDateTime.now()
                    );
                })
                .doOnNext(result -> logger.info("✅ Completed region {}: {} success, {} failures",
                        regionName, result.getProcessedCount(), result.getFailedCount()));
    }

    /**
     * 네이버 장소를 처리합니다 (구글 API 연동 + 설명 생성 + 저장)
     */
    private Mono<ProcessResult> processNaverPlace(NaverPlaceItem naverPlace) {
        logger.debug("🔄 Processing Naver place: {}", naverPlace.getTitle());

        return Mono.fromCallable(() -> {
            // 1. 중복 검사
            Optional<Place> existingPlace = placeRepository.findByNameAndAddress(
                    naverPlace.getTitle(), naverPlace.getAddress());

            if (existingPlace.isPresent()) {
                logger.debug("⏭️ Skipping duplicate place: {}", naverPlace.getTitle());
                return new ProcessResult(0, 1, "Duplicate place");
            }

            return null;
        })
        .flatMap(duplicateResult -> {
            if (duplicateResult != null) {
                return Mono.just(duplicateResult);
            }

            // 2. 구글 Places API로 상세 정보 가져오기
            return googlePlacesApiService.searchPlacesByText(naverPlace.getTitle() + " " + naverPlace.getAddress())
                    .next() // 첫 번째 결과만 사용
                    .flatMap(googlePlace -> googlePlacesApiService.getPlaceDetails(googlePlace.getPlaceId()))
                    .switchIfEmpty(Mono.empty())
                    .flatMap(googlePlace -> enrichAndSavePlace(naverPlace, googlePlace))
                    .switchIfEmpty(
                            // 구글 데이터가 없으면 네이버 데이터만으로 처리
                            enrichAndSavePlace(naverPlace, null)
                    );
        })
        .onErrorResume(error -> {
            logger.warn("❌ Failed to process place {}: {}", naverPlace.getTitle(), error.getMessage());
            return Mono.just(new ProcessResult(0, 1, "Processing error: " + error.getMessage()));
        });
    }

    /**
     * 장소 데이터 강화 및 저장
     */
    private Mono<ProcessResult> enrichAndSavePlace(NaverPlaceItem naverPlace, GooglePlaceDetail googlePlace) {
        return Mono.fromCallable(() -> {
            // 1. Place 엔티티 생성
            Place place = createPlaceFromData(naverPlace, googlePlace);

            // 2. 필터링 검사
            if (filterService.shouldExcludePlace(convertToProcessedPlace(place))) {
                logger.debug("🚫 Filtered out place: {}", place.getName());
                return new ProcessResult(0, 1, "Filtered out");
            }

            return place;
        })
        .flatMap(place -> {
            if (place instanceof ProcessResult) {
                return Mono.just((ProcessResult) place);
            }

            Place actualPlace = (Place) place;

            // 3. OpenAI로 설명 생성
            return generateDescription(actualPlace)
                    .flatMap(description -> {
                        actualPlace.setDescription(description);

                        // 4. 이미지 매핑
                        String imagePath = imageMappingService.getImagePath(actualPlace.getCategory());
                        actualPlace.setImageUrl(imagePath);

                        // 5. 데이터베이스 저장
                        return savePlace(actualPlace);
                    });
        });
    }

    /**
     * Place 엔티티 생성
     */
    private Place createPlaceFromData(NaverPlaceItem naverPlace, GooglePlaceDetail googlePlace) {
        Place.Builder builder = Place.builder()
                .name(naverPlace.getTitle())
                .address(naverPlace.getAddress())
                .roadAddress(naverPlace.getRoadAddress())
                .latitude(BigDecimal.valueOf(naverPlace.getLatitude()))
                .longitude(BigDecimal.valueOf(naverPlace.getLongitude()))
                .category(naverPlace.getCategory())
                .phone(naverPlace.getTelephone())
                .naverPlaceId(naverPlace.getLink())
                .sourceFlags("BATCH_NAVER");

        // 구글 데이터가 있으면 추가 정보 설정
        if (googlePlace != null) {
            builder.googlePlaceId(googlePlace.getPlaceId())
                    .rating(BigDecimal.valueOf(googlePlace.getRating()))
                    .reviewCount(googlePlace.getUserRatingsTotal())
                    .userRatingsTotal(googlePlace.getUserRatingsTotal())
                    .websiteUrl(googlePlace.getWebsite())
                    .types(googlePlace.getTypes().toArray(new String[0]))
                    .sourceFlags("BATCH_NAVER_GOOGLE");

            // 전화번호 우선순위: 구글 > 네이버
            if (googlePlace.getPhoneNumber() != null && !googlePlace.getPhoneNumber().isEmpty()) {
                builder.phone(googlePlace.getPhoneNumber());
            }
        }

        return builder.build();
    }

    /**
     * OpenAI를 통한 설명 생성
     */
    private Mono<String> generateDescription(Place place) {
        String prompt = String.format("""
                이 장소는 어떤 분위기인가요? 실제 사용자가 느끼는 따뜻하고 자연스러운 설명을 생성해주세요.
                문장은 사용자에게 말을 거는 듯한 친근한 톤이어야 하며, 이 장소를 방문하고 싶게 만들어야 합니다.
                네이버와 구글 리뷰를 참고해 분위기, 특색, 무드를 고려해 주세요.

                장소명: %s
                카테고리: %s
                주소: %s
                평점: %s
                """,
                place.getName(),
                place.getCategory(),
                place.getAddress(),
                place.getRating()
        );

        return enrichmentService.generateDescription(prompt)
                .switchIfEmpty(Mono.just("이곳은 특별한 매력을 가진 장소입니다. 방문해보시면 좋은 경험을 하실 수 있을 것 같아요!"))
                .doOnNext(desc -> logger.debug("📝 Generated description for {}: {} chars",
                        place.getName(), desc.length()));
    }

    /**
     * 장소 및 이미지 저장
     */
    @Transactional
    public Mono<ProcessResult> savePlace(Place place) {
        return Mono.fromCallable(() -> {
            try {
                // 1. Place 저장
                Place savedPlace = placeRepository.save(place);
                logger.debug("💾 Saved place: {} (ID: {})", savedPlace.getName(), savedPlace.getId());

                // 2. 이미지 저장 (기본 이미지)
                if (savedPlace.getImageUrl() != null) {
                    PlaceImage primaryImage = PlaceImage.builder()
                            .place(savedPlace)
                            .imageUrl(savedPlace.getImageUrl())
                            .imageType(PlaceImage.ImageType.GENERAL)
                            .isPrimary(true)
                            .displayOrder(0)
                            .source(PlaceImage.ImageSource.MANUAL_UPLOAD)
                            .isVerified(true)
                            .altText(savedPlace.getName() + " 기본 이미지")
                            .build();

                    placeImageRepository.save(primaryImage);
                    logger.debug("🖼️ Saved primary image for place: {}", savedPlace.getName());
                }

                return new ProcessResult(1, 0, "Successfully saved");

            } catch (Exception e) {
                logger.error("❌ Failed to save place {}: {}", place.getName(), e.getMessage());
                return new ProcessResult(0, 1, "Save error: " + e.getMessage());
            }
        });
    }

    /**
     * ProcessedPlaceJava로 변환 (필터링용)
     */
    private com.example.ingestion.dto.ProcessedPlaceJava convertToProcessedPlace(Place place) {
        com.example.ingestion.dto.ProcessedPlaceJava processed = new com.example.ingestion.dto.ProcessedPlaceJava();
        processed.setName(place.getName());
        processed.setCategory(place.getCategory());
        processed.setDescription(place.getDescription());
        processed.setAddress(place.getAddress());

        if (place.getTypes() != null) {
            processed.setTypes(Arrays.asList(place.getTypes()));
        }

        return processed;
    }

    /**
     * 배치 통계 조회
     */
    public Mono<BatchStatistics> getBatchStatistics() {
        return Mono.fromCallable(() -> {
            long totalPlaces = placeRepository.countAllPlaces();
            long totalImages = placeImageRepository.count();
            long batchPlaces = placeRepository.countBySourceFlags("BATCH_NAVER") +
                              placeRepository.countBySourceFlags("BATCH_NAVER_GOOGLE");

            return new BatchStatistics(
                    totalPlaces,
                    totalImages,
                    batchPlaces,
                    ZonedDateTime.now()
            );
        });
    }

    // Result classes
    public static class CollectionResult {
        private final String regionName;
        private final int processedCount;
        private final int failedCount;
        private final ZonedDateTime completedAt;

        public CollectionResult(String regionName, int processedCount, int failedCount, ZonedDateTime completedAt) {
            this.regionName = regionName;
            this.processedCount = processedCount;
            this.failedCount = failedCount;
            this.completedAt = completedAt;
        }

        public String getRegionName() { return regionName; }
        public int getProcessedCount() { return processedCount; }
        public int getFailedCount() { return failedCount; }
        public ZonedDateTime getCompletedAt() { return completedAt; }

        @Override
        public String toString() {
            return String.format("CollectionResult{region='%s', processed=%d, failed=%d}",
                    regionName, processedCount, failedCount);
        }
    }

    public static class ProcessResult {
        private final long processedCount;
        private final long failedCount;
        private final String message;

        public ProcessResult(long processedCount, long failedCount, String message) {
            this.processedCount = processedCount;
            this.failedCount = failedCount;
            this.message = message;
        }

        public long getProcessedCount() { return processedCount; }
        public long getFailedCount() { return failedCount; }
        public String getMessage() { return message; }

        @Override
        public String toString() {
            return String.format("ProcessResult{processed=%d, failed=%d, message='%s'}",
                    processedCount, failedCount, message);
        }
    }

    public static class BatchStatistics {
        private final long totalPlaces;
        private final long totalImages;
        private final long batchPlaces;
        private final ZonedDateTime generatedAt;

        public BatchStatistics(long totalPlaces, long totalImages, long batchPlaces, ZonedDateTime generatedAt) {
            this.totalPlaces = totalPlaces;
            this.totalImages = totalImages;
            this.batchPlaces = batchPlaces;
            this.generatedAt = generatedAt;
        }

        public long getTotalPlaces() { return totalPlaces; }
        public long getTotalImages() { return totalImages; }
        public long getBatchPlaces() { return batchPlaces; }
        public ZonedDateTime getGeneratedAt() { return generatedAt; }

        @Override
        public String toString() {
            return String.format("BatchStatistics{totalPlaces=%d, totalImages=%d, batchPlaces=%d}",
                    totalPlaces, totalImages, batchPlaces);
        }
    }
}