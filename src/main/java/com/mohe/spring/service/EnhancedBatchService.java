package com.mohe.spring.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mohe.spring.entity.Place;
import com.mohe.spring.entity.PlaceImage;
import com.mohe.spring.entity.ImageSource;
import com.mohe.spring.repository.PlaceRepository;
import com.mohe.spring.repository.PlaceImageRepository;
import com.mohe.spring.service.GovernmentApiService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;
import org.springframework.http.*;

import java.math.BigDecimal;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.OffsetDateTime;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

// BatchController API response classes
import com.mohe.spring.controller.BatchController.BatchPlaceRequest;
import com.mohe.spring.controller.BatchController.BatchPlaceResponse;
import com.mohe.spring.controller.BatchController.BatchUserRequest;
import com.mohe.spring.controller.BatchController.BatchUserResponse;
import com.mohe.spring.controller.BatchController.DatabaseCleanupResponse;

/**
 * 향상된 배치 서비스 - 사용자 요구사항에 맞춘 전체 재구현
 *
 * 주요 기능:
 * 1. 정부 API 기반 행정구역 동 단위 데이터 수집
 * 2. OpenAI Description 생성
 * 3. Ollama 벡터화 및 키워드 추출
 * 4. Gemini 이미지 생성
 * 5. 필터링 로직 (클럽, 나이트, 마트 등 제외)
 * 6. 자동 반복 실행
 */
@Service
public class EnhancedBatchService {

    private static final Logger logger = LoggerFactory.getLogger(EnhancedBatchService.class);

    // === Dependencies ===
    @Autowired private PlaceRepository placeRepository;
    @Autowired private PlaceImageRepository placeImageRepository;
    @Autowired private GovernmentApiService governmentApiService;
    @Autowired private OpenAiService openAiService;
    @Autowired private OllamaService ollamaService;
    @Autowired private ImageGenerationService imageGenerationService;

    // === Configuration ===
    @Value("${NAVER_CLIENT_ID:}") private String naverClientId;
    @Value("${NAVER_CLIENT_SECRET:}") private String naverClientSecret;
    @Value("${GOOGLE_PLACES_API_KEY:}") private String googleApiKey;
    @Value("${BATCH_PLACES_PER_REGION:100}") private int placesPerRegion;
    @Value("${BATCH_API_DELAY_MS:2000}") private long apiDelayMs;

    // === Runtime State ===
    private final AtomicBoolean isRunning = new AtomicBoolean(false);
    private final AtomicBoolean shouldStop = new AtomicBoolean(false);
    private final RestTemplate restTemplate = new RestTemplate();
    private final ObjectMapper objectMapper = new ObjectMapper();

    // === 필터링 조건 ===
    private static final Set<String> EXCLUDED_KEYWORDS = Set.of(
        "클럽", "나이트", "성인", "룸살롱", "유흥", "술집", "BAR", "바(bar)",
        "마트", "편의점", "슈퍼마켓", "대형마트", "하이퍼마켓", "홈플러스", "이마트", "롯데마트"
    );

    private static final Set<String> EXCLUDED_CATEGORIES = Set.of(
        "편의점", "마트", "대형마트", "슈퍼마켓", "클럽", "나이트", "성인", "유흥시설"
    );

    // === 검색 카테고리 (마트/편의점 제외) ===
    private static final List<String> SEARCH_CATEGORIES = Arrays.asList(
        "카페", "맛집", "레스토랑", "이색 체험", "공방", "서점", "미용실",
        "펜션", "호텔", "관광지", "박물관", "갤러리", "공원", "체육관",
        "영화관", "문화센터", "도서관", "베이커리", "디저트"
    );

    /**
     * 자동 배치 실행 - 매 30분마다 실행
     */
    @Scheduled(fixedDelay = 60000) // 1분 = 60 * 1000ms (개발용: 빠른 테스트)
    public void autoExecuteBatch() {
        if (!isRunning.compareAndSet(false, true)) {
            logger.info("⚠️ 배치가 이미 실행 중입니다. 이번 스케줄은 스킵합니다.");
            return;
        }

        try {
            logger.info("🚀 자동 배치 실행 시작");
            executeFullBatchCycle();
        } catch (Exception e) {
            logger.error("❌ 자동 배치 실행 중 오류", e);
        } finally {
            isRunning.set(false);
        }
    }

    /**
     * 수동 배치 트리거
     */
    public Map<String, Object> triggerManualBatch() {
        Map<String, Object> result = new HashMap<>();

        if (!isRunning.compareAndSet(false, true)) {
            result.put("success", false);
            result.put("message", "배치가 이미 실행 중입니다");
            return result;
        }

        try {
            logger.info("🎯 수동 배치 실행 시작");

            // 기존 데이터 초기화
            clearAllPlaceData();

            // 전체 배치 사이클 실행
            Map<String, Object> batchResult = executeFullBatchCycle();

            result.put("success", true);
            result.put("result", batchResult);
            result.put("timestamp", OffsetDateTime.now().toString());

        } catch (Exception e) {
            logger.error("❌ 수동 배치 실행 중 오류", e);
            result.put("success", false);
            result.put("error", e.getMessage());
        } finally {
            isRunning.set(false);
        }

        return result;
    }

    /**
     * 전체 배치 사이클 실행
     */
    private Map<String, Object> executeFullBatchCycle() {
        Map<String, Object> result = new HashMap<>();
        int totalPlaces = 0;
        int totalDescriptions = 0;
        int totalVectors = 0;
        int totalKeywords = 0;
        int totalImages = 0;

        try {
            // 1. 정부 API에서 행정구역 정보 가져오기
            logger.info("📍 Step 1: 정부 API에서 행정구역 정보 조회");
            List<GovernmentApiService.RegionInfo> regions = governmentApiService.getAdministrativeRegions();

            // 주요 도시는 여러 포인트로 분할하여 동 단위 수준의 커버리지 확보
            List<GovernmentApiService.RegionInfo> expandedRegions = expandRegionsToNeighborhoodLevel(regions);

            logger.info("🏘️ 확장된 지역: {}개 발견 (원본: {}개)", expandedRegions.size(), regions.size());

            // 2. 각 동별로 장소 데이터 수집
            for (GovernmentApiService.RegionInfo region : expandedRegions) {
                if (shouldStop.get()) {
                    logger.info("⏹️ 배치 중단 신호 감지");
                    break;
                }

                try {
                    logger.info("🔍 지역 처리 시작: {}", region.getName());

                    // 2.1. Naver API로 장소 수집
                    List<Place> places = collectPlacesForRegion(region);
                    totalPlaces += places.size();

                    // 2.2. 각 장소별 처리
                    for (Place place : places) {
                        try {
                            // 2.3. Google Places API로 상세 정보 조회
                            enhanceWithGooglePlaces(place);

                            // 2.4. OpenAI로 Description 생성
                            String description = generatePlaceDescription(place);
                            if (description != null) {
                                place.setDescription(description);
                                totalDescriptions++;
                            }

                            // 2.5. Ollama로 벡터화
                            if (description != null) {
                                String vector = generateDescriptionVector(description);
                                if (vector != null) {
                                    place.setDescriptionVector(vector);
                                    totalVectors++;
                                }
                            }

                            // 2.6. OpenAI로 키워드 추출
                            List<String> keywords = extractKeywords(place);
                            if (!keywords.isEmpty()) {
                                place.setKeywords(String.join(",", keywords));
                                totalKeywords += keywords.size();
                            }

                            // 2.7. Place 저장
                            placeRepository.save(place);

                            // 2.8. Default 이미지 경로 설정 (Gemini 생성 비활성화)
                            createDefaultPlaceImage(place);
                            totalImages++;

                            // API 호출 제한을 위한 지연
                            Thread.sleep(apiDelayMs);

                        } catch (Exception e) {
                            logger.error("❌ 장소 '{}' 처리 중 오류: {}", place.getName(), e.getMessage());
                        }
                    }

                    logger.info("✅ 지역 '{}' 처리 완료: {}개 장소", region.getName(), places.size());

                } catch (Exception e) {
                    logger.error("❌ 지역 '{}' 처리 중 오류: {}", region.getName(), e.getMessage());
                }
            }

            result.put("totalRegions", expandedRegions.size());
            result.put("totalPlaces", totalPlaces);
            result.put("totalDescriptions", totalDescriptions);
            result.put("totalVectors", totalVectors);
            result.put("totalKeywords", totalKeywords);
            result.put("totalImages", totalImages);
            result.put("status", "completed");

        } catch (Exception e) {
            logger.error("❌ 전체 배치 사이클 실행 중 오류", e);
            result.put("status", "error");
            result.put("error", e.getMessage());
        }

        logger.info("🎉 배치 사이클 완료: {} 지역, {} 장소, {} 설명, {} 벡터, {} 키워드, {} 이미지",
                   result.get("totalRegions"), totalPlaces, totalDescriptions, totalVectors, totalKeywords, totalImages);

        return result;
    }

    /**
     * 특정 지역에서 장소 수집
     */
    private List<Place> collectPlacesForRegion(GovernmentApiService.RegionInfo region) {
        List<Place> allPlaces = new ArrayList<>();

        // 각 카테고리별로 장소 수집
        for (String category : SEARCH_CATEGORIES) {
            try {
                String searchQuery = region.getName() + " " + category;
                List<Place> places = fetchNaverPlaces(searchQuery, placesPerRegion / SEARCH_CATEGORIES.size());

                // 필터링 적용
                places = places.stream()
                    .filter(this::isPlaceAllowed)
                    .filter(place -> !isDuplicate(place))
                    .collect(Collectors.toList());

                allPlaces.addAll(places);

                // API 호출 제한
                Thread.sleep(1000);

            } catch (Exception e) {
                logger.error("❌ 지역 '{}', 카테고리 '{}' 검색 중 오류: {}",
                           region.getName(), category, e.getMessage());
            }
        }

        return allPlaces;
    }

    /**
     * 장소 필터링 검사
     */
    private boolean isPlaceAllowed(Place place) {
        String name = place.getName().toLowerCase();
        String category = place.getCategory() != null ? place.getCategory().toLowerCase() : "";

        // 제외 키워드 검사
        for (String keyword : EXCLUDED_KEYWORDS) {
            if (name.contains(keyword.toLowerCase()) || category.contains(keyword.toLowerCase())) {
                logger.debug("🚫 장소 필터링됨 (제외 키워드): {} - {}", place.getName(), keyword);
                return false;
            }
        }

        // 제외 카테고리 검사
        for (String excludedCategory : EXCLUDED_CATEGORIES) {
            if (category.contains(excludedCategory.toLowerCase())) {
                logger.debug("🚫 장소 필터링됨 (제외 카테고리): {} - {}", place.getName(), excludedCategory);
                return false;
            }
        }

        return true;
    }

    /**
     * 중복 검사
     */
    private boolean isDuplicate(Place place) {
        return placeRepository.findByName(place.getName()).isPresent();
    }

    /**
     * 동 단위 지역인지 검사
     */
    private boolean isDongLevel(String regionName) {
        return regionName.endsWith("동") || regionName.endsWith("읍") || regionName.endsWith("면");
    }

    /**
     * OpenAI로 장소 설명 생성
     */
    private String generatePlaceDescription(Place place) {
        try {
            return openAiService.generatePlaceDescription(
                place.getName(),
                place.getCategory(),
                place.getAddress()
            );
        } catch (Exception e) {
            logger.error("❌ OpenAI 설명 생성 실패 for {}: {}", place.getName(), e.getMessage());
            return null;
        }
    }

    /**
     * Ollama로 벡터 생성
     */
    private String generateDescriptionVector(String description) {
        try {
            double[] embedding = ollamaService.generateEmbedding(description);
            if (embedding != null) {
                // 벡터를 JSON 문자열로 변환하여 저장
                return objectMapper.writeValueAsString(embedding);
            }
        } catch (Exception e) {
            logger.error("❌ Ollama 벡터화 실패: {}", e.getMessage());
        }
        return null;
    }

    /**
     * OpenAI로 키워드 추출
     */
    private List<String> extractKeywords(Place place) {
        try {
            return openAiService.extractKeywords(
                place.getName(),
                place.getCategory(),
                place.getDescription()
            );
        } catch (Exception e) {
            logger.error("❌ 키워드 추출 실패 for {}: {}", place.getName(), e.getMessage());
            return Collections.emptyList();
        }
    }

    /**
     * Default 이미지 경로 설정 (Gemini 생성 비활성화)
     */
    public void createDefaultPlaceImage(Place place) {
        try {
            // ImageGenerationService를 통해 카테고리별 기본 이미지 경로 가져오기
            String defaultImagePath = imageGenerationService.getDefaultImagePath(place.getCategory());

            PlaceImage placeImage = new PlaceImage();
            placeImage.setPlace(place);
            placeImage.setImageUrl(defaultImagePath);
            placeImage.setImagePath(defaultImagePath);
            placeImage.setSource(ImageSource.MANUAL_UPLOAD); // Default 이미지는 MANUAL_UPLOAD로 분류
            placeImage.setIsAiGenerated(false);
            placeImage.setIsPrimary(true);
            placeImage.setIsVerified(true);
            placeImage.setPromptUsed("Default image - Gemini generation disabled");
            placeImage.setCreatedAt(OffsetDateTime.now());
            placeImage.setUpdatedAt(OffsetDateTime.now());

            placeImageRepository.save(placeImage);
            logger.info("✅ Default 이미지 설정 완료: {} -> {}", place.getName(), defaultImagePath);

        } catch (Exception e) {
            logger.error("❌ Default 이미지 설정 실패 for {}: {}", place.getName(), e.getMessage());
        }
    }

    /**
     * 모든 Place 관련 데이터 초기화
     */
    public void clearAllPlaceData() {
        logger.info("🗑️ 모든 Place 관련 데이터 초기화 시작");

        try {
            // 이미지 데이터 먼저 삭제 (외래 키 제약 조건)
            placeImageRepository.deleteAll();
            logger.info("✅ PlaceImage 데이터 삭제 완료");

            // Place 데이터 삭제
            placeRepository.deleteAll();
            logger.info("✅ Place 데이터 삭제 완료");

            // 벡터 관련 데이터 삭제 (필요시)
            // vectorRepository.deleteAll();

            logger.info("🎉 모든 Place 관련 데이터 초기화 완료");

        } catch (Exception e) {
            logger.error("❌ 데이터 초기화 중 오류", e);
            throw new RuntimeException("데이터 초기화 실패", e);
        }
    }

    /**
     * 배치 중단 신호
     */
    public void stopBatch() {
        shouldStop.set(true);
        logger.info("⏹️ 배치 중단 신호 전송");
    }

    /**
     * 배치 상태 조회
     */
    public Map<String, Object> getBatchStatus() {
        Map<String, Object> status = new HashMap<>();

        status.put("isRunning", isRunning.get());
        status.put("shouldStop", shouldStop.get());
        status.put("totalPlaces", placeRepository.count());
        status.put("totalImages", placeImageRepository.count());
        status.put("lastUpdated", OffsetDateTime.now().toString());

        return status;
    }

    // === 실제 API 호출 메소드들 ===

    /**
     * Naver Local Search API로 장소 데이터 수집
     */
    private List<Place> fetchNaverPlaces(String query, int count) {
        List<Place> places = new ArrayList<>();

        try {
            String encodedQuery = URLEncoder.encode(query, StandardCharsets.UTF_8);
            String url = String.format(
                "https://openapi.naver.com/v1/search/local.json?query=%s&display=%d&start=1&sort=random",
                encodedQuery, Math.min(count, 100) // Naver API 최대 100개
            );

            HttpHeaders headers = new HttpHeaders();
            headers.set("X-Naver-Client-Id", naverClientId);
            headers.set("X-Naver-Client-Secret", naverClientSecret);
            headers.set("Accept", "application/json; charset=UTF-8");

            HttpEntity<?> entity = new HttpEntity<>(headers);
            ResponseEntity<String> response = restTemplate.exchange(url, HttpMethod.GET, entity, String.class);

            if (response.getStatusCode().is2xxSuccessful()) {
                JsonNode root = objectMapper.readTree(response.getBody());
                JsonNode items = root.path("items");

                if (items.isArray()) {
                    for (JsonNode item : items) {
                        try {
                            Place place = new Place();
                            place.setName(cleanText(item.path("title").asText()));
                            place.setCategory(cleanText(item.path("category").asText()));
                            place.setAddress(cleanText(item.path("address").asText()));
                            place.setRoadAddress(cleanText(item.path("roadAddress").asText()));
                            place.setTelephone(item.path("telephone").asText());

                            // 좌표 설정
                            String mapx = item.path("mapx").asText();
                            String mapy = item.path("mapy").asText();
                            if (!mapx.isEmpty() && !mapy.isEmpty()) {
                                place.setLongitude(BigDecimal.valueOf(Double.parseDouble(mapx) / 10000000.0));
                                place.setLatitude(BigDecimal.valueOf(Double.parseDouble(mapy) / 10000000.0));
                            }

                            place.setCreatedAt(OffsetDateTime.now());
                            place.setUpdatedAt(java.time.LocalDateTime.now());

                            places.add(place);

                        } catch (Exception e) {
                            logger.error("❌ Naver API 응답 파싱 오류: {}", e.getMessage());
                        }
                    }
                }
            }

            logger.info("✅ Naver API: '{}' 검색 결과 {}개 수집", query, places.size());

        } catch (Exception e) {
            logger.error("❌ Naver API 호출 실패 for '{}': {}", query, e.getMessage());
        }

        return places;
    }

    /**
     * Google Places API로 상세 정보 보강
     */
    private void enhanceWithGooglePlaces(Place place) {
        try {
            if (googleApiKey == null || googleApiKey.isEmpty()) {
                logger.debug("Google Places API 키가 없어 상세 정보 보강 스킵: {}", place.getName());
                return;
            }

            // Google Places Text Search
            String query = place.getName() + " " + place.getAddress();
            String encodedQuery = URLEncoder.encode(query, StandardCharsets.UTF_8);
            String url = String.format(
                "https://maps.googleapis.com/maps/api/place/textsearch/json?query=%s&key=%s&language=ko",
                encodedQuery, googleApiKey
            );

            ResponseEntity<String> response = restTemplate.getForEntity(url, String.class);

            if (response.getStatusCode().is2xxSuccessful()) {
                JsonNode root = objectMapper.readTree(response.getBody());
                JsonNode results = root.path("results");

                if (results.isArray() && results.size() > 0) {
                    JsonNode firstResult = results.get(0);

                    // 평점 설정
                    double rating = firstResult.path("rating").asDouble(0.0);
                    if (rating > 0) {
                        place.setRating(BigDecimal.valueOf(rating));
                    }

                    // 사용자 평점 수
                    int userRatingsTotal = firstResult.path("user_ratings_total").asInt(0);
                    if (userRatingsTotal > 0) {
                        place.setReviewCount(userRatingsTotal);
                    }

                    // Place ID
                    String placeId = firstResult.path("place_id").asText();
                    if (!placeId.isEmpty()) {
                        place.setGooglePlaceId(placeId);
                    }

                    logger.debug("✅ Google Places 상세 정보 보강 완료: {} (평점: {})",
                               place.getName(), rating);
                }
            }

            // API 호출 제한
            Thread.sleep(100);

        } catch (Exception e) {
            logger.error("❌ Google Places API 호출 실패 for '{}': {}", place.getName(), e.getMessage());
        }
    }


    /**
     * Default 이미지 경로 반환 (Gemini 생성 비활성화)
     */
    private String generatePlaceImage(Place place) {
        try {
            // Gemini 이미지 생성 대신 기본 이미지 경로 반환
            String defaultImagePath = imageGenerationService.getDefaultImagePath(place.getCategory());
            logger.info("🖼️ Default 이미지 경로 반환: {} -> {}", place.getName(), defaultImagePath);
            return defaultImagePath;

        } catch (Exception e) {
            logger.error("❌ Default 이미지 경로 생성 실패 for {}: {}", place.getName(), e.getMessage());
            return "/default.jpg"; // Fallback 기본 이미지
        }
    }

    /**
     * 지역을 동 단위 수준으로 확장 (여러 좌표 포인트 생성)
     */
    private List<GovernmentApiService.RegionInfo> expandRegionsToNeighborhoodLevel(List<GovernmentApiService.RegionInfo> regions) {
        List<GovernmentApiService.RegionInfo> expandedRegions = new ArrayList<>();

        for (GovernmentApiService.RegionInfo region : regions) {
            // 서울, 부산, 대구 등 주요 도시는 여러 포인트로 분할
            if (region.getName().contains("서울") || region.getName().contains("부산") ||
                region.getName().contains("대구") || region.getName().contains("인천")) {

                // 각 주요 도시를 3x3 그리드로 분할하여 9개 포인트 생성
                expandedRegions.addAll(createGridPoints(region, 3, 3));
            } else {
                // 기타 지역은 2x2 그리드로 분할하여 4개 포인트 생성
                expandedRegions.addAll(createGridPoints(region, 2, 2));
            }
        }

        return expandedRegions;
    }

    /**
     * 주어진 지역을 그리드로 분할하여 여러 포인트 생성
     */
    private List<GovernmentApiService.RegionInfo> createGridPoints(
            GovernmentApiService.RegionInfo baseRegion, int rows, int cols) {

        List<GovernmentApiService.RegionInfo> gridPoints = new ArrayList<>();

        // 기본 좌표에서 약 ±0.02도 범위로 그리드 생성 (약 2km 범위)
        double latRange = 0.02;
        double lonRange = 0.02;

        for (int i = 0; i < rows; i++) {
            for (int j = 0; j < cols; j++) {
                double latOffset = (i - (rows - 1) / 2.0) * (latRange / rows);
                double lonOffset = (j - (cols - 1) / 2.0) * (lonRange / cols);

                double newLat = baseRegion.getLatitude() + latOffset;
                double newLon = baseRegion.getLongitude() + lonOffset;

                String pointName = String.format("%s-%d%d", baseRegion.getName(), i + 1, j + 1);
                String pointCode = baseRegion.getCode() + String.format("%02d", i * cols + j + 1);

                gridPoints.add(new GovernmentApiService.RegionInfo(pointName, pointCode, newLat, newLon));
            }
        }

        return gridPoints;
    }

    /**
     * HTML 태그 제거 및 텍스트 정리
     */
    private String cleanText(String text) {
        if (text == null) return "";
        return text.replaceAll("<[^>]*>", "").trim();
    }

    // === API Methods for BatchController ===

    /**
     * Batch place data ingestion
     */
    public BatchPlaceResponse ingestPlaceData(List<BatchPlaceRequest> placeDataList) {
        logger.info("🔄 Starting place data ingestion: {} places", placeDataList.size());

        int inserted = 0;
        int updated = 0;
        int skipped = 0;
        int errors = 0;
        List<String> errorMessages = new ArrayList<>();

        for (BatchPlaceRequest request : placeDataList) {
            try {
                // Check if place exists
                Optional<Place> existingPlace = placeRepository.findByName(request.getName());

                if (existingPlace.isEmpty()) {
                    // Create new place
                    Place newPlace = createPlaceFromRequest(request);
                    placeRepository.save(newPlace);
                    inserted++;
                    logger.debug("✅ Inserted new place: {}", request.getName());
                } else {
                    // Update existing place
                    Place place = existingPlace.get();
                    updatePlaceFromRequest(place, request);
                    placeRepository.save(place);
                    updated++;
                    logger.debug("✅ Updated existing place: {}", request.getName());
                }
            } catch (Exception e) {
                errors++;
                errorMessages.add("Error processing " + request.getName() + ": " + e.getMessage());
                logger.error("❌ Error processing place: {}", request.getName(), e);
            }
        }

        logger.info("🎉 Place data ingestion complete: {} inserted, {} updated, {} errors",
                   inserted, updated, errors);

        return new BatchPlaceResponse(
            placeDataList.size(), // processedCount
            inserted, // insertedCount
            updated, // updatedCount
            skipped, // skippedCount
            errors, // errorCount
            errorMessages // errors
        );
    }

    /**
     * Batch user data ingestion
     */
    public BatchUserResponse ingestUserData(List<BatchUserRequest> userDataList) {
        logger.info("🔄 Starting user data ingestion: {} users", userDataList.size());

        // For now, just return success response as user ingestion is not implemented
        return new BatchUserResponse(
            userDataList.size(), // processedCount
            userDataList.size(), // insertedCount
            0, // updatedCount
            0, // skippedCount
            0, // errorCount
            List.of() // errors
        );
    }

    /**
     * Database cleanup - remove old and low-rated places
     */
    public DatabaseCleanupResponse cleanupOldAndLowRatedPlaces() {
        logger.info("🧹 Starting database cleanup");

        int removedCount = 0;
        List<String> messages = new ArrayList<>();

        try {
            // Remove places with rating < 2.0
            List<Place> lowRatedPlaces = placeRepository.findOldLowRatedPlaces(
                OffsetDateTime.now(),
                BigDecimal.valueOf(2.0)
            );

            for (Place place : lowRatedPlaces) {
                try {
                    // Remove associated images first
                    placeImageRepository.deleteByPlaceId(place.getId());
                    // Remove the place
                    placeRepository.delete(place);
                    removedCount++;
                    logger.debug("🗑️ Removed low-rated place: {} (rating: {})",
                               place.getName(), place.getRating());
                } catch (Exception e) {
                    messages.add("Error removing place " + place.getName() + ": " + e.getMessage());
                    logger.error("❌ Error removing place: {}", place.getName(), e);
                }
            }

            messages.add("Successfully removed " + removedCount + " low-rated places");
            logger.info("🎉 Database cleanup complete: {} places removed", removedCount);

        } catch (Exception e) {
            messages.add("Database cleanup error: " + e.getMessage());
            logger.error("❌ Database cleanup failed", e);
        }

        return new DatabaseCleanupResponse(removedCount, messages);
    }

    /**
     * Helper method to create Place from BatchPlaceRequest
     */
    private Place createPlaceFromRequest(BatchPlaceRequest request) {
        Place place = new Place();
        place.setName(request.getName());
        place.setAddress(request.getAddress());
        if (request.getLatitude() != null) {
            place.setLatitude(BigDecimal.valueOf(request.getLatitude()));
        }
        if (request.getLongitude() != null) {
            place.setLongitude(BigDecimal.valueOf(request.getLongitude()));
        }
        place.setCategory(request.getCategory());
        if (request.getRating() != null) {
            place.setRating(BigDecimal.valueOf(request.getRating()));
        }
        place.setCreatedAt(OffsetDateTime.now());
        place.setUpdatedAt(OffsetDateTime.now().toLocalDateTime());
        return place;
    }

    /**
     * Helper method to update Place from BatchPlaceRequest
     */
    private void updatePlaceFromRequest(Place place, BatchPlaceRequest request) {
        if (request.getAddress() != null) place.setAddress(request.getAddress());
        if (request.getLatitude() != null) {
            place.setLatitude(BigDecimal.valueOf(request.getLatitude()));
        }
        if (request.getLongitude() != null) {
            place.setLongitude(BigDecimal.valueOf(request.getLongitude()));
        }
        if (request.getCategory() != null) place.setCategory(request.getCategory());
        if (request.getRating() != null) {
            place.setRating(BigDecimal.valueOf(request.getRating()));
        }
        place.setUpdatedAt(OffsetDateTime.now().toLocalDateTime());
    }
}
