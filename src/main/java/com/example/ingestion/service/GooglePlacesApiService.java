package com.example.ingestion.service;

import com.example.ingestion.dto.GooglePlaceDetail;
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
import java.util.stream.Collectors;

/**
 * Google Places API 서비스
 * 장소 상세 정보, 평점, 리뷰, 사진 등을 수집합니다
 */
@Service
public class GooglePlacesApiService {

    private static final Logger logger = LoggerFactory.getLogger(GooglePlacesApiService.class);

    private final WebClient webClient;
    private final String googleApiKey;

    public GooglePlacesApiService(
            WebClient webClient,
            @Value("${GOOGLE_PLACES_API_KEY}") String googleApiKey
    ) {
        this.webClient = webClient;
        this.googleApiKey = googleApiKey;
    }

    /**
     * 텍스트 검색으로 장소를 찾습니다
     * @param query 검색 쿼리 (예: "강남구 카페")
     * @return 검색된 장소 목록
     */
    public Flux<GooglePlaceDetail> searchPlacesByText(String query) {
        logger.debug("🔍 Searching Google Places with query: '{}'", query);

        return webClient.get()
                .uri(uriBuilder -> uriBuilder
                        .scheme("https")
                        .host("maps.googleapis.com")
                        .path("/maps/api/place/textsearch/json")
                        .queryParam("query", query)
                        .queryParam("key", googleApiKey)
                        .queryParam("language", "ko")
                        .queryParam("region", "kr")
                        .build())
                .retrieve()
                .bodyToMono(JsonNode.class)
                .retryWhen(Retry.backoff(3, Duration.ofSeconds(2))
                        .maxBackoff(Duration.ofSeconds(10))
                        .doBeforeRetry(retrySignal ->
                                logger.warn("Retrying Google Places text search, attempt: {}",
                                        retrySignal.totalRetries() + 1)))
                .flux()
                .flatMapIterable(this::parseTextSearchResponse)
                .doOnNext(place -> logger.debug("Found Google place: {} ({})",
                        place.getName(), place.getPlaceId()))
                .onErrorResume(error -> {
                    logger.error("Google Places text search failed for query '{}': {}",
                            query, error.getMessage());
                    return Flux.empty();
                });
    }

    /**
     * Place ID로 상세 정보를 가져옵니다
     * @param placeId Google Place ID
     * @return 장소 상세 정보
     */
    public Mono<GooglePlaceDetail> getPlaceDetails(String placeId) {
        logger.debug("📍 Fetching Google Place details for ID: {}", placeId);

        String fields = "place_id,name,formatted_address,geometry," +
                       "rating,user_ratings_total,price_level,types," +
                       "opening_hours,formatted_phone_number,website," +
                       "photos,reviews,vicinity,business_status";

        return webClient.get()
                .uri(uriBuilder -> uriBuilder
                        .scheme("https")
                        .host("maps.googleapis.com")
                        .path("/maps/api/place/details/json")
                        .queryParam("place_id", placeId)
                        .queryParam("fields", fields)
                        .queryParam("key", googleApiKey)
                        .queryParam("language", "ko")
                        .queryParam("region", "kr")
                        .build())
                .retrieve()
                .bodyToMono(JsonNode.class)
                .retryWhen(Retry.backoff(3, Duration.ofSeconds(2))
                        .maxBackoff(Duration.ofSeconds(10)))
                .map(this::parseDetailResponse)
                .filter(detail -> detail != null)
                .doOnNext(detail -> logger.debug("Fetched details for: {} (rating: {})",
                        detail.getName(), detail.getRating()))
                .onErrorResume(error -> {
                    logger.error("Failed to fetch Google Place details for ID '{}': {}",
                            placeId, error.getMessage());
                    return Mono.empty();
                });
    }

    /**
     * 근처 장소 검색 (좌표 기반)
     * @param latitude 위도
     * @param longitude 경도
     * @param radius 검색 반경 (미터)
     * @param type 장소 유형 (선택사항)
     * @return 근처 장소 목록
     */
    public Flux<GooglePlaceDetail> searchNearbyPlaces(double latitude, double longitude,
                                                     int radius, String type) {
        logger.debug("🗺️ Searching nearby places at {},{} within {}m radius",
                latitude, longitude, radius);

        return webClient.get()
                .uri(uriBuilder -> {
                    var builder = uriBuilder
                            .scheme("https")
                            .host("maps.googleapis.com")
                            .path("/maps/api/place/nearbysearch/json")
                            .queryParam("location", latitude + "," + longitude)
                            .queryParam("radius", radius)
                            .queryParam("key", googleApiKey)
                            .queryParam("language", "ko");

                    if (type != null && !type.trim().isEmpty()) {
                        builder.queryParam("type", type);
                    }

                    return builder.build();
                })
                .retrieve()
                .bodyToMono(JsonNode.class)
                .retryWhen(Retry.backoff(3, Duration.ofSeconds(2))
                        .maxBackoff(Duration.ofSeconds(10)))
                .flux()
                .flatMapIterable(this::parseNearbySearchResponse)
                .doOnNext(place -> logger.debug("Found nearby place: {} at distance from {},{}}",
                        place.getName(), latitude, longitude));
    }

    /**
     * 사진 URL을 가져옵니다
     * @param photoReference Google Places 사진 참조
     * @param maxWidth 최대 너비
     * @return 사진 URL
     */
    public String getPhotoUrl(String photoReference, int maxWidth) {
        if (photoReference == null || photoReference.trim().isEmpty()) {
            return null;
        }

        return String.format(
                "https://maps.googleapis.com/maps/api/place/photo?maxwidth=%d&photo_reference=%s&key=%s",
                maxWidth, photoReference, googleApiKey
        );
    }

    /**
     * 텍스트 검색 응답 파싱
     */
    private List<GooglePlaceDetail> parseTextSearchResponse(JsonNode response) {
        List<GooglePlaceDetail> places = new ArrayList<>();

        String status = response.path("status").asText();
        if (!"OK".equals(status)) {
            logger.warn("Google Places API returned status: {}", status);
            if ("OVER_QUERY_LIMIT".equals(status)) {
                logger.error("Google Places API quota exceeded!");
            }
            return places;
        }

        JsonNode results = response.path("results");
        if (!results.isArray()) {
            logger.warn("Results field is not an array");
            return places;
        }

        for (JsonNode result : results) {
            try {
                GooglePlaceDetail place = parseBasicPlaceData(result);
                if (place != null && isValidPlace(place)) {
                    places.add(place);
                }
            } catch (Exception e) {
                logger.warn("Error parsing Google place result: {}", e.getMessage());
            }
        }

        logger.debug("Parsed {} valid places from Google text search", places.size());
        return places;
    }

    /**
     * 근처 검색 응답 파싱
     */
    private List<GooglePlaceDetail> parseNearbySearchResponse(JsonNode response) {
        return parseTextSearchResponse(response); // 동일한 구조
    }

    /**
     * 상세 정보 응답 파싱
     */
    private GooglePlaceDetail parseDetailResponse(JsonNode response) {
        String status = response.path("status").asText();
        if (!"OK".equals(status)) {
            logger.warn("Google Place details API returned status: {}", status);
            return null;
        }

        JsonNode result = response.path("result");
        if (result.isMissingNode()) {
            logger.warn("No result field in place details response");
            return null;
        }

        return parseBasicPlaceData(result);
    }

    /**
     * 기본 장소 데이터 파싱
     */
    private GooglePlaceDetail parseBasicPlaceData(JsonNode placeNode) {
        try {
            String placeId = placeNode.path("place_id").asText();
            String name = placeNode.path("name").asText();
            String address = placeNode.path("formatted_address").asText();

            // 좌표 파싱
            double latitude = 0.0;
            double longitude = 0.0;
            JsonNode geometry = placeNode.path("geometry");
            if (!geometry.isMissingNode()) {
                JsonNode location = geometry.path("location");
                latitude = location.path("lat").asDouble();
                longitude = location.path("lng").asDouble();
            }

            // 평점 및 리뷰 수
            double rating = placeNode.path("rating").asDouble(0.0);
            int userRatingsTotal = placeNode.path("user_ratings_total").asInt(0);
            int priceLevel = placeNode.path("price_level").asInt(0);

            // 타입들
            List<String> types = new ArrayList<>();
            JsonNode typesNode = placeNode.path("types");
            if (typesNode.isArray()) {
                for (JsonNode type : typesNode) {
                    types.add(type.asText());
                }
            }

            // 연락처 및 웹사이트
            String phoneNumber = placeNode.path("formatted_phone_number").asText("");
            String website = placeNode.path("website").asText("");

            // 사진들
            List<String> photoReferences = new ArrayList<>();
            JsonNode photos = placeNode.path("photos");
            if (photos.isArray()) {
                for (JsonNode photo : photos) {
                    String photoRef = photo.path("photo_reference").asText();
                    if (!photoRef.isEmpty()) {
                        photoReferences.add(photoRef);
                    }
                }
            }

            // 영업시간
            String openingHours = "";
            JsonNode openingHoursNode = placeNode.path("opening_hours");
            if (!openingHoursNode.isMissingNode()) {
                JsonNode weekdayText = openingHoursNode.path("weekday_text");
                if (weekdayText.isArray()) {
                    List<String> hours = new ArrayList<>();
                    for (JsonNode day : weekdayText) {
                        hours.add(day.asText());
                    }
                    openingHours = String.join("; ", hours);
                }
            }

            // 리뷰들 (최대 5개)
            List<String> reviews = new ArrayList<>();
            JsonNode reviewsNode = placeNode.path("reviews");
            if (reviewsNode.isArray()) {
                int reviewCount = 0;
                for (JsonNode review : reviewsNode) {
                    if (reviewCount >= 5) break;
                    String reviewText = review.path("text").asText();
                    if (!reviewText.isEmpty()) {
                        reviews.add(reviewText);
                        reviewCount++;
                    }
                }
            }

            return GooglePlaceDetail.builder()
                    .placeId(placeId)
                    .name(name)
                    .address(address)
                    .latitude(latitude)
                    .longitude(longitude)
                    .rating(rating)
                    .userRatingsTotal(userRatingsTotal)
                    .priceLevel(priceLevel)
                    .types(types)
                    .phoneNumber(phoneNumber)
                    .website(website)
                    .photoReferences(photoReferences)
                    .openingHours(openingHours)
                    .reviews(reviews)
                    .build();

        } catch (Exception e) {
            logger.error("Error parsing Google place data: {}", e.getMessage());
            return null;
        }
    }

    /**
     * 유효한 장소인지 검증
     */
    private boolean isValidPlace(GooglePlaceDetail place) {
        if (place == null) {
            return false;
        }

        // 필수 필드 검증
        if (place.getName() == null || place.getName().trim().isEmpty()) {
            logger.debug("Invalid Google place: missing name");
            return false;
        }

        if (place.getPlaceId() == null || place.getPlaceId().trim().isEmpty()) {
            logger.debug("Invalid Google place: missing place ID");
            return false;
        }

        // 좌표 검증 (한국 영역)
        if (place.getLatitude() < 33.0 || place.getLatitude() > 39.0 ||
            place.getLongitude() < 124.0 || place.getLongitude() > 132.0) {
            logger.debug("Invalid Google place: coordinates out of Korea range for {}",
                    place.getName());
            return false;
        }

        return true;
    }

    /**
     * API 사용량 통계
     */
    public Mono<ApiUsageStats> getApiUsageStats() {
        return Mono.fromCallable(() -> new ApiUsageStats(
                "Google Places API",
                googleApiKey != null && !googleApiKey.equals("your_real_google_api_key_here")
        ));
    }

    /**
     * API 사용량 통계 클래스
     */
    public static class ApiUsageStats {
        private final String serviceName;
        private final boolean isConfigured;

        public ApiUsageStats(String serviceName, boolean isConfigured) {
            this.serviceName = serviceName;
            this.isConfigured = isConfigured;
        }

        public String getServiceName() { return serviceName; }
        public boolean isConfigured() { return isConfigured; }

        @Override
        public String toString() {
            return String.format("ApiUsageStats{service='%s', configured=%s}",
                    serviceName, isConfigured);
        }
    }
}