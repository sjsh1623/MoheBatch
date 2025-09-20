package com.example.ingestion.service;

import com.example.ingestion.dto.NaverPlaceItem;
import com.fasterxml.jackson.databind.JsonNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Naver Local Search API Service
 * 네이버 지역 검색 API를 통해 장소 정보를 수집합니다
 */
@Service
public class NaverApiService {

    private static final Logger logger = LoggerFactory.getLogger(NaverApiService.class);

    private final WebClient webClient;
    private final String naverClientId;
    private final String naverClientSecret;

    // 검색 카테고리 목록
    private static final List<String> SEARCH_CATEGORIES = List.of(
            "카페", "커피숍", "맛집", "음식점", "한식", "중식", "일식", "양식",
            "베이커리", "디저트", "술집", "바", "펜션", "호텔", "게스트하우스",
            "박물관", "미술관", "갤러리", "극장", "공연장", "체험관",
            "공원", "놀이공원", "테마파크", "수목원", "동물원",
            "도서관", "문화센터", "스파", "찜질방", "운동시설"
    );

    public NaverApiService(
            WebClient webClient,
            @Value("${NAVER_CLIENT_ID}") String naverClientId,
            @Value("${NAVER_CLIENT_SECRET}") String naverClientSecret
    ) {
        this.webClient = webClient;
        this.naverClientId = naverClientId;
        this.naverClientSecret = naverClientSecret;
    }

    /**
     * 특정 지역의 모든 카테고리 장소를 검색합니다
     * @param region 검색할 지역 (예: "강남구 논현동")
     * @return 검색된 장소 목록
     */
    public Flux<NaverPlaceItem> searchPlacesByRegion(String region) {
        logger.info("🔍 Starting place search for region: {}", region);

        return Flux.fromIterable(SEARCH_CATEGORIES)
                .delayElements(Duration.ofMillis(200)) // Rate limiting
                .flatMap(category -> searchPlacesByQuery(region + " " + category))
                .distinct(NaverPlaceItem::getTitle) // Remove duplicates by title
                .doOnNext(place -> logger.debug("Found place: {} in {}", place.getTitle(), region))
                .doOnComplete(() -> logger.info("✅ Completed place search for region: {}", region));
    }

    /**
     * 특정 쿼리로 장소를 검색합니다
     * @param query 검색 쿼리
     * @return 검색된 장소 목록
     */
    public Flux<NaverPlaceItem> searchPlacesByQuery(String query) {
        return searchPlacesByQuery(query, 100); // Default 100 results
    }

    /**
     * 특정 쿼리로 장소를 검색합니다 (결과 수 제한)
     * @param query 검색 쿼리
     * @param maxResults 최대 결과 수
     * @return 검색된 장소 목록
     */
    public Flux<NaverPlaceItem> searchPlacesByQuery(String query, int maxResults) {
        int display = Math.min(maxResults, 5); // Naver API limit per request
        int totalPages = (int) Math.ceil((double) maxResults / display);

        logger.debug("Searching places with query: '{}', maxResults: {}, pages: {}", query, maxResults, totalPages);

        return Flux.range(1, totalPages)
                .delayElements(Duration.ofMillis(300)) // Rate limiting between pages
                .flatMap(page -> {
                    int start = (page - 1) * display + 1;
                    return fetchNaverPlaces(query, display, start)
                            .flux()
                            .flatMapIterable(this::parseNaverResponse);
                })
                .take(maxResults)
                .doOnError(error -> logger.error("Error searching places with query '{}': {}", query, error.getMessage()));
    }

    /**
     * 네이버 API에서 장소 검색 결과를 가져옵니다
     */
    private Mono<JsonNode> fetchNaverPlaces(String query, int display, int start) {
        return webClient.get()
                .uri(uriBuilder -> uriBuilder
                        .scheme("https")
                        .host("openapi.naver.com")
                        .path("/v1/search/local.json")
                        .queryParam("query", query)
                        .queryParam("display", display)
                        .queryParam("start", start)
                        .queryParam("sort", "random") // 다양한 결과를 위해 랜덤 정렬
                        .build())
                .header("X-Naver-Client-Id", naverClientId)
                .header("X-Naver-Client-Secret", naverClientSecret)
                .header("User-Agent", "MoheBatch/1.0")
                .retrieve()
                .bodyToMono(JsonNode.class)
                .retryWhen(Retry.backoff(3, Duration.ofSeconds(2))
                        .maxBackoff(Duration.ofSeconds(10))
                        .doBeforeRetry(retrySignal ->
                                logger.warn("Retrying Naver API request for query '{}', attempt: {}",
                                        query, retrySignal.totalRetries() + 1)))
                .doOnNext(response -> {
                    if (response.has("total")) {
                        int total = response.get("total").asInt();
                        logger.debug("Naver API returned {} total results for query: '{}'", total, query);
                    }
                })
                .onErrorResume(error -> {
                    logger.error("Naver API request failed for query '{}': {}", query, error.getMessage());
                    return Mono.empty();
                });
    }

    /**
     * 네이버 API 응답을 파싱하여 NaverPlaceItem 목록으로 변환합니다
     */
    private List<NaverPlaceItem> parseNaverResponse(JsonNode response) {
        List<NaverPlaceItem> places = new ArrayList<>();

        if (!response.has("items")) {
            logger.warn("No items found in Naver API response");
            return places;
        }

        JsonNode items = response.get("items");
        if (!items.isArray()) {
            logger.warn("Items field is not an array in Naver API response");
            return places;
        }

        for (JsonNode item : items) {
            try {
                NaverPlaceItem place = parseNaverPlaceItem(item);
                if (place != null && isValidPlace(place)) {
                    places.add(place);
                }
            } catch (Exception e) {
                logger.warn("Error parsing Naver place item: {}", e.getMessage());
            }
        }

        logger.debug("Parsed {} valid places from Naver response", places.size());
        return places;
    }

    /**
     * 개별 네이버 장소 아이템을 파싱합니다
     */
    private NaverPlaceItem parseNaverPlaceItem(JsonNode item) {
        try {
            String title = cleanHtmlTags(item.path("title").asText());
            String category = cleanHtmlTags(item.path("category").asText());
            String address = cleanHtmlTags(item.path("address").asText());
            String roadAddress = cleanHtmlTags(item.path("roadAddress").asText());
            String telephone = item.path("telephone").asText();
            String link = item.path("link").asText();

            // 좌표 파싱
            double latitude = 0.0;
            double longitude = 0.0;
            try {
                String mapx = item.path("mapx").asText();
                String mapy = item.path("mapy").asText();
                if (!mapx.isEmpty() && !mapy.isEmpty()) {
                    longitude = Double.parseDouble(mapx) / 10000000.0; // Naver format conversion
                    latitude = Double.parseDouble(mapy) / 10000000.0;
                }
            } catch (NumberFormatException e) {
                logger.warn("Invalid coordinates in Naver data for place: {}", title);
            }

            return NaverPlaceItem.builder()
                    .title(title)
                    .category(category)
                    .address(address)
                    .roadAddress(roadAddress)
                    .telephone(telephone)
                    .link(link)
                    .latitude(latitude)
                    .longitude(longitude)
                    .build();

        } catch (Exception e) {
            logger.error("Error parsing Naver place item: {}", e.getMessage());
            return null;
        }
    }

    /**
     * HTML 태그를 제거합니다 (네이버 API는 검색어를 강조 표시함)
     */
    private String cleanHtmlTags(String text) {
        if (text == null || text.trim().isEmpty()) {
            return "";
        }
        return text.replaceAll("<[^>]*>", "").trim();
    }

    /**
     * 유효한 장소인지 검증합니다
     */
    private boolean isValidPlace(NaverPlaceItem place) {
        if (place == null) {
            return false;
        }

        // 필수 필드 검증
        if (place.getTitle() == null || place.getTitle().trim().isEmpty()) {
            logger.debug("Invalid place: missing title");
            return false;
        }

        if (place.getAddress() == null || place.getAddress().trim().isEmpty()) {
            logger.debug("Invalid place: missing address for {}", place.getTitle());
            return false;
        }

        // 좌표 검증 (한국 영역 대략적 범위)
        if (place.getLatitude() < 33.0 || place.getLatitude() > 39.0 ||
            place.getLongitude() < 124.0 || place.getLongitude() > 132.0) {
            logger.debug("Invalid place: coordinates out of Korea range for {}", place.getTitle());
            return false;
        }

        return true;
    }

    /**
     * 검색 통계를 가져옵니다
     */
    public Mono<SearchStats> getSearchStats() {
        return Mono.fromCallable(() -> new SearchStats(
                SEARCH_CATEGORIES.size(),
                SEARCH_CATEGORIES
        ));
    }

    /**
     * 검색 통계 클래스
     */
    public static class SearchStats {
        private final int totalCategories;
        private final List<String> categories;

        public SearchStats(int totalCategories, List<String> categories) {
            this.totalCategories = totalCategories;
            this.categories = categories;
        }

        public int getTotalCategories() { return totalCategories; }
        public List<String> getCategories() { return categories; }

        @Override
        public String toString() {
            return String.format("SearchStats{totalCategories=%d, categories=%s}",
                    totalCategories, categories);
        }
    }
}