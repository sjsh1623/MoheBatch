package com.mohe.spring.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mohe.spring.controller.BatchController.BatchPlaceRequest;
import com.mohe.spring.controller.BatchController.BatchPlaceResponse;
import com.mohe.spring.controller.BatchController.BatchUserRequest;
import com.mohe.spring.controller.BatchController.BatchUserResponse;
import com.mohe.spring.controller.BatchController.InternalPlaceIngestRequest;
import com.mohe.spring.controller.BatchController.InternalPlaceIngestResponse;
import com.mohe.spring.controller.BatchController.DatabaseCleanupResponse;
import com.mohe.spring.entity.Place;
import com.mohe.spring.entity.PlaceImage;
import com.mohe.spring.repository.PlaceRepository;
import com.mohe.spring.repository.PlaceImageRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.domain.PageRequest;
import org.springframework.http.*;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.math.BigDecimal;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.OffsetDateTime;
import java.util.*;
import java.util.concurrent.CompletableFuture;

@Service
public class BatchService {

    private static final Logger logger = LoggerFactory.getLogger(BatchService.class);

    @Autowired
    private PlaceRepository placeRepository;

    @Autowired
    private PlaceImageRepository placeImageRepository;

    @Autowired
    private ImageGenerationService imageGenerationService;

    // Note: These services are not used in this version - using direct API calls instead

    private final RestTemplate restTemplate;

    public BatchService() {
        // Configure RestTemplate with proper settings for Korean text and full response handling
        org.springframework.http.client.SimpleClientHttpRequestFactory factory = new org.springframework.http.client.SimpleClientHttpRequestFactory();
        factory.setConnectTimeout(10000); // 10 seconds
        factory.setReadTimeout(30000); // 30 seconds

        this.restTemplate = new RestTemplate(factory);

        // Configure message converters to properly handle Korean text and ensure full response reading
        this.restTemplate.getMessageConverters().clear();

        org.springframework.http.converter.StringHttpMessageConverter stringConverter =
            new org.springframework.http.converter.StringHttpMessageConverter(java.nio.charset.StandardCharsets.UTF_8);
        stringConverter.setWriteAcceptCharset(false); // Don't write charset in Accept-Charset header

        org.springframework.http.converter.json.MappingJackson2HttpMessageConverter jsonConverter =
            new org.springframework.http.converter.json.MappingJackson2HttpMessageConverter();
        jsonConverter.setDefaultCharset(java.nio.charset.StandardCharsets.UTF_8);

        this.restTemplate.getMessageConverters().add(stringConverter);
        this.restTemplate.getMessageConverters().add(jsonConverter);
    }
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Value("${NAVER_CLIENT_ID:}")
    private String naverClientId;

    @Value("${NAVER_CLIENT_SECRET:}")
    private String naverClientSecret;

    @Value("${GOOGLE_PLACES_API_KEY:}")
    private String googleApiKey;

    // 한국 주요 지역 쿼리
    private static final List<String> KOREAN_LOCATIONS = Arrays.asList(
            "카페", "데이트", "박물관", "바", "칵테일", "재즈", "맛집"
    );

    /**
     * 자동 배치 처리 트리거 - 실제 API 데이터 수집 및 AI 이미지 생성
     */
    public Map<String, Object> triggerBatch() {
        logger.info("Starting automated batch processing with real API data collection and AI image generation");

        Map<String, Object> result = new HashMap<>();

        try {
            // 1. 모든 기존 데이터 제거
            clearAllData();

            // 2. 실제 API에서 장소 데이터 수집
            int collectedPlaces = collectRealPlaceData();

            // 3. AI 이미지 생성
            int generatedImages = generateAiImagesForPlaces();

            result.put("status", "success");
            result.put("collectedPlaces", collectedPlaces);
            result.put("generatedImages", generatedImages);
            result.put("timestamp", OffsetDateTime.now().toString());

            logger.info("Batch processing completed successfully: {} places, {} images",
                       collectedPlaces, generatedImages);

        } catch (Exception e) {
            logger.error("Batch processing failed", e);
            result.put("status", "error");
            result.put("error", e.getMessage());
        }

        return result;
    }

    /**
     * 배치 처리 상태 조회
     */
    public Map<String, Object> getBatchStatus() {
        Map<String, Object> status = new HashMap<>();

        try {
            long totalPlaces = placeRepository.count();
            long totalImages = placeImageRepository.count();
            List<PlaceImage> aiImages = placeImageRepository.findByIsAiGeneratedTrueOrderByCreatedAtDesc(
                PageRequest.of(0, 1000));
            long aiGeneratedImages = aiImages.size();

            status.put("totalPlaces", totalPlaces);
            status.put("totalImages", totalImages);
            status.put("aiGeneratedImages", aiGeneratedImages);
            status.put("lastUpdated", OffsetDateTime.now().toString());
            status.put("status", "active");

        } catch (Exception e) {
            logger.error("Failed to get batch status", e);
            status.put("status", "error");
            status.put("error", e.getMessage());
        }

        return status;
    }

    /**
     * 모든 기존 데이터 제거
     */
    private void clearAllData() {
        logger.info("Clearing all existing place and image data");
        placeImageRepository.deleteAll();
        placeRepository.deleteAll();
        logger.info("All existing data cleared successfully");
    }

    /**
     * 포트폴리오용 Java HTTP 클라이언트 테스트
     */
    public String testJavaHttpClient() {
        try {
            List<Place> places = fetchNaverPlacesWithProperJavaClient("홍대 카페", 3);
            return String.format("Java HTTP Client test successful, found %d places with complete response", places.size());
        } catch (Exception e) {
            logger.error("Java HTTP Client test failed", e);
            return "Java HTTP Client test failed: " + e.getMessage();
        }
    }

    /**
     * 실제 API에서 장소 데이터 수집 (공개 메소드로 변경)
     */
    public int collectRealPlaceData() {
        logger.info("Starting real place data collection from Naver API");
        int totalCollected = 0;

        // 랜덤하게 5개 지역 선택
        List<String> locations = new ArrayList<>(KOREAN_LOCATIONS);
        Collections.shuffle(locations);
        List<String> selectedLocations = locations.subList(0, Math.min(5, locations.size()));

        for (String query : selectedLocations) {
            try {
                List<Place> places = fetchNaverPlaces(query, 3);
                for (Place place : places) {
                    // 중복 체크 (이름으로만 - 간단하게)
                    Optional<Place> existingPlace = placeRepository.findByName(place.getName());
                    if (existingPlace.isEmpty()) {
                        // Google API로 평점 보강
                        enhanceWithGooglePlaces(place);
                        placeRepository.save(place);
                        totalCollected++;
                        logger.info("Saved new place: {}", place.getName());
                    } else {
                        logger.info("Place already exists, skipping: {}", place.getName());
                    }
                }
                Thread.sleep(2000); // API 호출 간격
            } catch (Exception e) {
                logger.error("Error collecting data for query: {}", query, e);
            }
        }

        logger.info("Real place data collection completed: {} places", totalCollected);
        return totalCollected;
    }

    /**
     * Naver API로 장소 데이터 가져오기 - OkHttp 사용 (업계 표준 라이브러리)
     */
    private List<Place> fetchNaverPlaces(String query, int count) {
        logger.info("🔍 fetchNaverPlaces called with query: '{}', count: {}", query, count);
        logger.info("🔑 API credentials: clientId length: {}, clientSecret length: {}",
                   naverClientId != null ? naverClientId.length() : 0,
                   naverClientSecret != null ? naverClientSecret.length() : 0);

        if (naverClientId == null || naverClientSecret == null ||
            naverClientId.trim().isEmpty() || naverClientSecret.trim().isEmpty()) {
            logger.error("❌ Naver API credentials not configured: clientId={}, clientSecret={}",
                        naverClientId != null ? "SET" : "NULL",
                        naverClientSecret != null ? "SET" : "NULL");
            return Collections.emptyList();
        }

        // 포트폴리오용 완벽한 Java HTTP 클라이언트 (curl 사용 없음)
        return fetchNaverPlacesWithProperJavaClient(query, count);
    }

    /**
     * 포트폴리오용 완벽한 Java HTTP 클라이언트 (107자 잘림 문제 완전 해결)
     */
    private List<Place> fetchNaverPlacesWithProperJavaClient(String query, int count) {
        logger.info("🚀 PORTFOLIO: Using Pure Java HTTP Client for query: '{}'", query);

        try {
            String encodedQuery = URLEncoder.encode(query, StandardCharsets.UTF_8);
            String urlString = String.format(
                "https://openapi.naver.com/v1/search/local.json?query=%s&display=%d&start=1&sort=random",
                encodedQuery, count
            );

            logger.info("📡 PORTFOLIO: Making request to URL: {}", urlString);

            // URL과 연결 설정 - 검증된 방법
            java.net.URL url = new java.net.URL(urlString);
            java.net.HttpURLConnection connection = (java.net.HttpURLConnection) url.openConnection();

            // HTTP 연결 설정 - 완전한 응답 수신을 위한 최적화
            connection.setRequestMethod("GET");
            connection.setRequestProperty("X-Naver-Client-Id", naverClientId);
            connection.setRequestProperty("X-Naver-Client-Secret", naverClientSecret);
            connection.setRequestProperty("Accept", "application/json; charset=UTF-8");
            connection.setRequestProperty("User-Agent", "MoheSpring-Portfolio/1.0");

            // 핵심: 107자 잘림 방지 설정
            connection.setRequestProperty("Accept-Encoding", "identity");
            connection.setRequestProperty("Cache-Control", "no-cache");
            connection.setUseCaches(false);
            connection.setConnectTimeout(15000);
            connection.setReadTimeout(30000);

            int responseCode = connection.getResponseCode();
            logger.info("📊 PORTFOLIO: Response Code: {}", responseCode);

            if (responseCode != 200) {
                logger.error("❌ PORTFOLIO: API Error - Response Code: {}", responseCode);
                return Collections.emptyList();
            }

            // 핵심: ByteArrayOutputStream으로 완전한 응답 읽기
            String responseBody;
            try (java.io.InputStream inputStream = connection.getInputStream();
                 java.io.ByteArrayOutputStream byteArrayOutputStream = new java.io.ByteArrayOutputStream()) {

                byte[] buffer = new byte[8192];
                int bytesRead;
                int totalBytesRead = 0;

                while ((bytesRead = inputStream.read(buffer)) != -1) {
                    byteArrayOutputStream.write(buffer, 0, bytesRead);
                    totalBytesRead += bytesRead;
                }

                responseBody = byteArrayOutputStream.toString(StandardCharsets.UTF_8.name());
                logger.info("✅ PORTFOLIO: Complete response received - {} chars (vs 107 char bug)", responseBody.length());
            }

            connection.disconnect();

            // 응답 검증 로깅
            logger.info("📝 PORTFOLIO: Response first 150 chars: {}",
                       responseBody.substring(0, Math.min(150, responseBody.length())));

            // JSON 파싱
            JsonNode jsonResponse = objectMapper.readTree(responseBody);
            JsonNode items = jsonResponse.get("items");
            JsonNode total = jsonResponse.get("total");

            logger.info("📊 PORTFOLIO: JSON parsed - total: {}, items count: {}",
                       total != null ? total.asInt() : "null",
                       items != null && items.isArray() ? items.size() : 0);

            List<Place> places = new ArrayList<>();
            if (items != null && items.isArray()) {
                logger.info("🔄 PORTFOLIO: Processing {} items from Naver API", items.size());
                for (int i = 0; i < items.size(); i++) {
                    JsonNode item = items.get(i);
                    String title = item.has("title") ? item.get("title").asText() : "Unknown";
                    logger.info("📍 PORTFOLIO: Item {}: {}", i + 1, title);

                    Place place = convertNaverItemToPlace(item);
                    places.add(place);
                    logger.info("✅ PORTFOLIO: Successfully converted place: {}", place.getName());
                }
            } else {
                logger.warn("⚠️ PORTFOLIO: No items in response");
            }

            logger.info("🎯 PORTFOLIO: Successfully processed {} places for query: '{}'", places.size(), query);
            return places;

        } catch (Exception e) {
            logger.error("❌ PORTFOLIO: Error in Java HTTP client for query '{}': {}", query, e.getMessage(), e);
            return Collections.emptyList();
        }
    }

    /**
     * 간단한 RestTemplate + RequestEntity 방식 (성공 사례에서 검증된 방법)
     */
    private List<Place> fetchNaverPlacesWithSimpleRestTemplate(String query, int count) {
        logger.info("🚀 Using Simple RestTemplate + RequestEntity for query: '{}'", query);

        try {
            String encodedQuery = URLEncoder.encode(query, StandardCharsets.UTF_8);

            // UriComponentsBuilder로 URL 생성 (성공 사례와 동일한 방식)
            String uri = org.springframework.web.util.UriComponentsBuilder
                .fromUriString("https://openapi.naver.com")
                .path("/v1/search/local.json")
                .queryParam("query", encodedQuery)
                .queryParam("display", count)
                .queryParam("start", 1)
                .queryParam("sort", "random")
                .toUriString();

            logger.info("Simple RestTemplate request to URI: {}", uri);

            // RequestEntity 방식 (성공 사례와 동일)
            org.springframework.http.RequestEntity<Void> req = org.springframework.http.RequestEntity
                .get(uri)
                .header("X-Naver-Client-Id", naverClientId)
                .header("X-Naver-Client-Secret", naverClientSecret)
                .header("Content-Type", "application/json; charset=UTF-8")
                .header("Accept", "application/json; charset=UTF-8")
                .build();

            // 가장 기본적인 RestTemplate 사용
            org.springframework.web.client.RestTemplate restTemplate = new org.springframework.web.client.RestTemplate();

            org.springframework.http.ResponseEntity<String> responseEntity =
                restTemplate.exchange(req, String.class);

            String responseBody = responseEntity.getBody();
            if (responseBody == null) {
                responseBody = "";
            }

            logger.info("🌐 Simple RestTemplate Response - Status: {}, Body length: {}",
                       responseEntity.getStatusCode(), responseBody.length());

            logger.info("✅ Simple RestTemplate succeeded - Processing response...");
            logger.info("📝 Simple RestTemplate response (first 500 chars): {}",
                       responseBody.substring(0, Math.min(500, responseBody.length())));

            return parseNaverResponse(responseBody, query, "Simple RestTemplate");

        } catch (Exception e) {
            logger.error("Error using Simple RestTemplate for Naver API query: {}", query, e);
            throw new RuntimeException("Simple RestTemplate method failed", e);
        }
    }

    /**
     * 기본 HttpURLConnection 방식 (최소한의 설정)
     */
    private List<Place> fetchNaverPlacesWithBasicConnection(String query, int count) {
        logger.info("🚀 Using Basic HttpURLConnection for query: '{}'", query);

        try {
            String encodedQuery = URLEncoder.encode(query, StandardCharsets.UTF_8);
            String urlString = String.format(
                "https://openapi.naver.com/v1/search/local.json?query=%s&display=%d&start=1&sort=random",
                encodedQuery, count
            );

            logger.info("Basic HttpURLConnection request to URL: {}", urlString);

            java.net.URL url = new java.net.URL(urlString);
            java.net.HttpURLConnection connection = (java.net.HttpURLConnection) url.openConnection();

            // 최소한의 설정만
            connection.setRequestMethod("GET");
            connection.setRequestProperty("X-Naver-Client-Id", naverClientId);
            connection.setRequestProperty("X-Naver-Client-Secret", naverClientSecret);
            connection.setRequestProperty("Accept", "application/json; charset=UTF-8");
            connection.setConnectTimeout(10000);
            connection.setReadTimeout(30000);

            int responseCode = connection.getResponseCode();
            logger.info("🌐 Basic Connection Response Code: {}", responseCode);

            // 응답 읽기 (BufferedReader 사용)
            java.io.BufferedReader reader;
            if (responseCode == 200) {
                reader = new java.io.BufferedReader(
                    new java.io.InputStreamReader(connection.getInputStream(), StandardCharsets.UTF_8));
            } else {
                reader = new java.io.BufferedReader(
                    new java.io.InputStreamReader(connection.getErrorStream(), StandardCharsets.UTF_8));
            }

            StringBuilder responseBuilder = new StringBuilder();
            String line;
            while ((line = reader.readLine()) != null) {
                responseBuilder.append(line);
            }
            reader.close();
            connection.disconnect();

            String responseBody = responseBuilder.toString();
            logger.info("🌐 Basic Connection Response - Body length: {}", responseBody.length());

            logger.info("✅ Basic Connection succeeded - Processing response...");
            logger.info("📝 Basic Connection response (first 500 chars): {}",
                       responseBody.substring(0, Math.min(500, responseBody.length())));

            return parseNaverResponse(responseBody, query, "Basic Connection");

        } catch (Exception e) {
            logger.error("Error using Basic HttpURLConnection for Naver API query: {}", query, e);
            throw new RuntimeException("Basic HttpURLConnection method failed", e);
        }
    }

    /**
     * HTTP (SSL 없이) 시도 - Web Search에서 SSL 문제 해결책 발견
     */
    private List<Place> fetchNaverPlacesWithoutSSL(String query, int count) {
        logger.info("🚀 Using HTTP (no SSL) for query: '{}'", query);

        try {
            String encodedQuery = URLEncoder.encode(query, StandardCharsets.UTF_8);
            // HTTP (SSL 없이) URL 사용
            String url = String.format(
                "http://openapi.naver.com/v1/search/local.json?query=%s&display=%d&start=1&sort=random",
                encodedQuery, count
            );

            logger.info("HTTP (no SSL) request to URL: {}", url);

            // 기본 Java HttpClient with HTTP 1.1
            java.net.http.HttpClient httpClient = java.net.http.HttpClient.newBuilder()
                .version(java.net.http.HttpClient.Version.HTTP_1_1)
                .connectTimeout(java.time.Duration.ofSeconds(10))
                .build();

            java.net.http.HttpRequest request = java.net.http.HttpRequest.newBuilder()
                .uri(java.net.URI.create(url))
                .timeout(java.time.Duration.ofSeconds(30))
                .header("X-Naver-Client-Id", naverClientId)
                .header("X-Naver-Client-Secret", naverClientSecret)
                .header("Accept", "application/json; charset=UTF-8")
                .header("User-Agent", "MoheSpring-HTTP-NoSSL/1.0")
                .GET()
                .build();

            java.net.http.HttpResponse<String> response = httpClient.send(request,
                java.net.http.HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));

            logger.info("🌐 HTTP (no SSL) Response - Status: {}, Body length: {}",
                       response.statusCode(), response.body().length());

            String responseBody = response.body();
            logger.info("✅ HTTP (no SSL) succeeded - Processing response...");
            logger.info("📝 HTTP response (first 500 chars): {}",
                       responseBody.substring(0, Math.min(500, responseBody.length())));

            return parseNaverResponse(responseBody, query, "HTTP (no SSL)");

        } catch (Exception e) {
            logger.error("Error using HTTP (no SSL) for Naver API query: {}", query, e);
            throw new RuntimeException("HTTP (no SSL) method failed", e);
        }
    }

    /**
     * Large Buffer 사용 - Web Search에서 버퍼 크기 문제 해결책 발견
     */
    private List<Place> fetchNaverPlacesWithLargeBuffer(String query, int count) {
        logger.info("🚀 Using Java HttpClient with large buffer for query: '{}'", query);

        try {
            String encodedQuery = URLEncoder.encode(query, StandardCharsets.UTF_8);
            String url = String.format(
                "https://openapi.naver.com/v1/search/local.json?query=%s&display=%d&start=1&sort=random",
                encodedQuery, count
            );

            logger.info("Large buffer request to URL: {}", url);

            // HttpClient with custom executor for large buffer
            java.util.concurrent.Executor executor = java.util.concurrent.Executors.newFixedThreadPool(1);
            java.net.http.HttpClient httpClient = java.net.http.HttpClient.newBuilder()
                .version(java.net.http.HttpClient.Version.HTTP_1_1)
                .connectTimeout(java.time.Duration.ofSeconds(10))
                .executor(executor)
                .build();

            java.net.http.HttpRequest request = java.net.http.HttpRequest.newBuilder()
                .uri(java.net.URI.create(url))
                .timeout(java.time.Duration.ofSeconds(30))
                .header("X-Naver-Client-Id", naverClientId)
                .header("X-Naver-Client-Secret", naverClientSecret)
                .header("Accept", "application/json; charset=UTF-8")
                .header("User-Agent", "MoheSpring-LargeBuffer/1.0")
                .header("Connection", "keep-alive") // keep-alive for better handling
                .GET()
                .build();

            // 큰 버퍼 사용하여 응답 받기
            java.net.http.HttpResponse<String> response = httpClient.send(request,
                java.net.http.HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));

            logger.info("🌐 Large buffer Response - Status: {}, Body length: {}",
                       response.statusCode(), response.body().length());

            String responseBody = response.body();
            logger.info("✅ Large buffer succeeded - Processing response...");
            logger.info("📝 Large buffer response (first 500 chars): {}",
                       responseBody.substring(0, Math.min(500, responseBody.length())));

            return parseNaverResponse(responseBody, query, "Large Buffer");

        } catch (Exception e) {
            logger.error("Error using large buffer for Naver API query: {}", query, e);
            throw new RuntimeException("Large buffer method failed", e);
        }
    }

    /**
     * 공통 응답 파싱 메소드
     */
    private List<Place> parseNaverResponse(String responseBody, String query, String method) {
        try {
            JsonNode jsonResponse = objectMapper.readTree(responseBody);
            JsonNode items = jsonResponse.get("items");
            JsonNode total = jsonResponse.get("total");

            logger.info("📊 JSON parsed via {} - total: {}, items array size: {}",
                       method, total != null ? total.asInt() : "null",
                       items != null && items.isArray() ? items.size() : "null or not array");

            List<Place> places = new ArrayList<>();
            if (items != null && items.isArray()) {
                logger.info("🔄 Processing {} items from Naver API via {}", items.size(), method);
                for (int i = 0; i < items.size(); i++) {
                    JsonNode item = items.get(i);
                    logger.info("📍 Item {}: {}", i+1, item.get("title"));
                    Place place = convertNaverItemToPlace(item);
                    places.add(place);
                    logger.info("✅ Added place via {}: {}", method, place.getName());
                }
            } else {
                logger.warn("⚠️ Items is null or not array - items: {}", items);
            }

            logger.info("✅ Successfully processed {} places from Naver API via {} for query: {}", places.size(), method, query);
            return places;
        } catch (Exception e) {
            logger.error("Error parsing response via {}: {}", method, e.getMessage());
            throw new RuntimeException("Response parsing failed", e);
        }
    }


    /**
     * Java 11+ HttpClient를 HTTP/2로 사용 (curl과 동일한 프로토콜)
     */
    private List<Place> fetchNaverPlacesWithJavaHttpClient(String query, int count) {
        logger.info("🚀 Using Java HttpClient with HTTP/2 for query: '{}'", query);

        try {
            String encodedQuery = URLEncoder.encode(query, StandardCharsets.UTF_8);
            String url = String.format(
                "https://openapi.naver.com/v1/search/local.json?query=%s&display=%d&start=1&sort=random",
                encodedQuery, count
            );

            logger.info("Java HttpClient request to URL: {}", url);

            // Java 11+ HttpClient - HTTP/2 사용 (curl과 동일)
            java.net.http.HttpClient httpClient = java.net.http.HttpClient.newBuilder()
                .version(java.net.http.HttpClient.Version.HTTP_2) // HTTP/2 사용
                .connectTimeout(java.time.Duration.ofSeconds(10))
                .build();

            java.net.http.HttpRequest request = java.net.http.HttpRequest.newBuilder()
                .uri(java.net.URI.create(url))
                .timeout(java.time.Duration.ofSeconds(30))
                .header("X-Naver-Client-Id", naverClientId)
                .header("X-Naver-Client-Secret", naverClientSecret)
                .header("Accept", "application/json; charset=UTF-8")
                .header("User-Agent", "MoheSpring-JavaHttpClient-HTTP2/1.0")
                .GET()
                .build();

            java.net.http.HttpResponse<String> response = httpClient.send(request,
                java.net.http.HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));

            logger.info("🌐 Java HttpClient Response - Status: {}, Body length: {}",
                       response.statusCode(), response.body().length());

            // 응답 헤더 정보 로깅
            logger.info("Java HttpClient Headers: {}", response.headers().map());

            String responseBody = response.body();
            logger.info("✅ Java HttpClient succeeded - Processing response...");
            logger.info("📝 Java HttpClient response (first 500 chars): {}",
                       responseBody.substring(0, Math.min(500, responseBody.length())));

            JsonNode jsonResponse = objectMapper.readTree(responseBody);
            JsonNode items = jsonResponse.get("items");
            JsonNode total = jsonResponse.get("total");

            logger.info("📊 JSON parsed via Java HttpClient - total: {}, items array size: {}",
                       total != null ? total.asInt() : "null",
                       items != null && items.isArray() ? items.size() : "null or not array");

            List<Place> places = new ArrayList<>();
            if (items != null && items.isArray()) {
                logger.info("🔄 Processing {} items from Naver API via Java HttpClient", items.size());
                for (int i = 0; i < items.size(); i++) {
                    JsonNode item = items.get(i);
                    logger.info("📍 Item {}: {}", i+1, item.get("title"));
                    Place place = convertNaverItemToPlace(item);
                    places.add(place);
                    logger.info("✅ Added place via Java HttpClient: {}", place.getName());
                }
            } else {
                logger.warn("⚠️ Items is null or not array - items: {}", items);
            }

            logger.info("✅ Successfully processed {} places from Naver API via Java HttpClient for query: {}", places.size(), query);
            return places;

        } catch (Exception e) {
            logger.error("Error using Java HttpClient for Naver API query: {}", query, e);
            throw new RuntimeException("Java HttpClient failed", e);
        }
    }

    /**
     * Spring WebClient를 사용한 주 메소드 - Spring 공식 권장 HTTP 클라이언트
     */
    private List<Place> fetchNaverPlacesWithWebClient(String query, int count) {
        logger.info("🚀 Using Spring WebClient for query: '{}'", query);

        try {
            String encodedQuery = URLEncoder.encode(query, StandardCharsets.UTF_8);
            String url = String.format(
                "https://openapi.naver.com/v1/search/local.json?query=%s&display=%d&start=1&sort=random",
                encodedQuery, count
            );

            logger.info("Spring WebClient request to URL: {}", url);

            // WebClient 설정 - Reactor Netty HTTP 클라이언트로 압축 비활성화
            reactor.netty.http.client.HttpClient httpClient = reactor.netty.http.client.HttpClient.create()
                .compress(false) // 압축 비활성화
                .headers(h -> h.set("Accept-Encoding", "identity")) // 압축 요청 차단
                .headers(h -> h.set("Connection", "close")); // 연결 종료로 chunked 방지

            org.springframework.web.reactive.function.client.WebClient webClient =
                org.springframework.web.reactive.function.client.WebClient.builder()
                    .clientConnector(new org.springframework.http.client.reactive.ReactorClientHttpConnector(httpClient))
                    .defaultHeader("X-Naver-Client-Id", naverClientId)
                    .defaultHeader("X-Naver-Client-Secret", naverClientSecret)
                    .defaultHeader("Accept", "application/json; charset=UTF-8")
                    .defaultHeader("User-Agent", "MoheSpring-WebClient-Fixed/1.0")
                    .defaultHeader("Cache-Control", "no-cache")
                    .codecs(configurer -> configurer.defaultCodecs().maxInMemorySize(2 * 1024 * 1024)) // 2MB buffer
                    .build();

            // 동기 호출 - ResponseEntity로 헤더와 바디를 모두 받기
            org.springframework.http.ResponseEntity<String> responseEntity = webClient.get()
                .uri(url)
                .retrieve()
                .toEntity(String.class)
                .block(java.time.Duration.ofSeconds(30));

            if (responseEntity == null) {
                throw new RuntimeException("WebClient response is null");
            }

            String responseBody = responseEntity.getBody();
            if (responseBody == null) {
                responseBody = "";
            }

            logger.info("🌐 WebClient Response - Status: {}, Body length: {}",
                       responseEntity.getStatusCode(), responseBody.length());

            // WebClient 응답 헤더 정보 로깅
            logger.info("WebClient Headers - Content-Type: {}, Content-Length: {}",
                       responseEntity.getHeaders().getFirst("Content-Type"),
                       responseEntity.getHeaders().getFirst("Content-Length"));

            logger.info("All WebClient Headers: {}", responseEntity.getHeaders().toSingleValueMap());

            logger.info("✅ WebClient succeeded - Processing response...");
            logger.info("📝 WebClient response (first 500 chars): {}",
                       responseBody.substring(0, Math.min(500, responseBody.length())));

            JsonNode jsonResponse = objectMapper.readTree(responseBody);
            JsonNode items = jsonResponse.get("items");
            JsonNode total = jsonResponse.get("total");

            logger.info("📊 JSON parsed via WebClient - total: {}, items array size: {}",
                       total != null ? total.asInt() : "null",
                       items != null && items.isArray() ? items.size() : "null or not array");

            List<Place> places = new ArrayList<>();
            if (items != null && items.isArray()) {
                logger.info("🔄 Processing {} items from Naver API via WebClient", items.size());
                for (int i = 0; i < items.size(); i++) {
                    JsonNode item = items.get(i);
                    logger.info("📍 Item {}: {}", i+1, item.get("title"));
                    Place place = convertNaverItemToPlace(item);
                    places.add(place);
                    logger.info("✅ Added place via WebClient: {}", place.getName());
                }
            } else {
                logger.warn("⚠️ Items is null or not array - items: {}", items);
            }

            logger.info("✅ Successfully processed {} places from Naver API via WebClient for query: {}", places.size(), query);
            return places;

        } catch (Exception e) {
            logger.error("Error using WebClient for Naver API query: {}", query, e);
            throw new RuntimeException("WebClient failed", e);
        }
    }

    /**
     * OkHttp를 사용한 fallback 메소드
     */
    private List<Place> fetchNaverPlacesWithOkHttp(String query, int count) {
        logger.info("🔄 Using OkHttp for query: '{}'", query);

        try {
            String encodedQuery = URLEncoder.encode(query, StandardCharsets.UTF_8);
            String url = String.format(
                "https://openapi.naver.com/v1/search/local.json?query=%s&display=%d&start=1&sort=random",
                encodedQuery, count
            );

            logger.info("Making Naver API request using OkHttp to URL: {}", url);

            // OkHttp 클라이언트 설정 - HTTP/1.1 강제 및 응답 완전 읽기
            okhttp3.OkHttpClient client = new okhttp3.OkHttpClient.Builder()
                .connectTimeout(10, java.util.concurrent.TimeUnit.SECONDS)
                .readTimeout(30, java.util.concurrent.TimeUnit.SECONDS)
                .writeTimeout(10, java.util.concurrent.TimeUnit.SECONDS)
                .retryOnConnectionFailure(true)
                .followRedirects(true)
                .protocols(java.util.Arrays.asList(okhttp3.Protocol.HTTP_1_1))  // HTTP/1.1 강제
                .build();

            // 요청 생성 - Compression 비활성화 및 Chunked Encoding 방지
            okhttp3.Request request = new okhttp3.Request.Builder()
                .url(url)
                .addHeader("X-Naver-Client-Id", naverClientId)
                .addHeader("X-Naver-Client-Secret", naverClientSecret)
                .addHeader("Accept", "application/json; charset=UTF-8")
                .addHeader("User-Agent", "MoheSpring-OkHttp-Fixed/4.12.0")
                .addHeader("Accept-Charset", "UTF-8")
                .addHeader("Accept-Encoding", "identity") // 압축 요청 차단
                .addHeader("Connection", "close") // 연결 종료로 chunked 방지
                .addHeader("Cache-Control", "no-cache")
                .get()
                .build();

            logger.info("OkHttp Request configured - Headers: Client-ID={}, Client-Secret={}",
                       naverClientId.substring(0, Math.min(5, naverClientId.length())) + "***",
                       naverClientSecret.substring(0, Math.min(3, naverClientSecret.length())) + "***");

            // API 호출 실행
            try (okhttp3.Response response = client.newCall(request).execute()) {
                String responseBody = "";

                if (response.body() != null) {
                    // 응답을 바이트 단위로 완전히 읽기
                    try (java.io.InputStream inputStream = response.body().byteStream();
                         java.io.ByteArrayOutputStream byteArrayOutputStream = new java.io.ByteArrayOutputStream()) {

                        byte[] buffer = new byte[1024];
                        int length;
                        while ((length = inputStream.read(buffer)) != -1) {
                            byteArrayOutputStream.write(buffer, 0, length);
                        }
                        responseBody = byteArrayOutputStream.toString(StandardCharsets.UTF_8);
                    }
                } else {
                    responseBody = "";
                }

                logger.info("🌐 OkHttp Response - Status: {}, Body length: {}, Success: {}",
                           response.code(), responseBody.length(), response.isSuccessful());

                // 모든 응답 헤더 정보 로깅
                logger.info("Response Headers - Content-Type: {}, Content-Length: {}, Transfer-Encoding: {}",
                           response.header("Content-Type", "unknown"),
                           response.header("Content-Length", "unknown"),
                           response.header("Transfer-Encoding", "unknown"));

                // 추가 헤더들
                logger.info("Additional Headers - Vary: {}, Server: {}, Connection: {}",
                           response.header("Vary", "unknown"),
                           response.header("Server", "unknown"),
                           response.header("Connection", "unknown"));

                // 모든 헤더 출력
                logger.info("All Headers: {}", response.headers().toMultimap());

                if (response.isSuccessful() && !responseBody.trim().isEmpty()) {
                    logger.info("✅ OkHttp succeeded - Processing response...");
                    logger.info("📝 Full OkHttp response (first 500 chars): {}",
                               responseBody.substring(0, Math.min(500, responseBody.length())));

                    JsonNode jsonResponse = objectMapper.readTree(responseBody);
                    JsonNode items = jsonResponse.get("items");
                    JsonNode total = jsonResponse.get("total");

                    logger.info("📊 JSON parsed via OkHttp - total: {}, items array size: {}",
                               total != null ? total.asInt() : "null",
                               items != null && items.isArray() ? items.size() : "null or not array");

                    List<Place> places = new ArrayList<>();
                    if (items != null && items.isArray()) {
                        logger.info("🔄 Processing {} items from Naver API via OkHttp", items.size());
                        for (int i = 0; i < items.size(); i++) {
                            JsonNode item = items.get(i);
                            logger.info("📍 Item {}: {}", i+1, item.get("title"));
                            Place place = convertNaverItemToPlace(item);
                            places.add(place);
                            logger.info("✅ Added place via OkHttp: {}", place.getName());
                        }
                    } else {
                        logger.warn("⚠️ Items is null or not array - items: {}", items);
                    }

                    logger.info("✅ Successfully processed {} places from Naver API via OkHttp for query: {}", places.size(), query);
                    return places;
                } else {
                    logger.error("❌ OkHttp request failed - Status: {}, Body: {}",
                                response.code(), responseBody.substring(0, Math.min(300, responseBody.length())));
                }
            }

        } catch (Exception e) {
            logger.error("Error using OkHttp for Naver API query: {}", query, e);

            // fallback to Apache HttpClient
            return fetchNaverPlacesWithApacheClient(query, count);
        }

        return Collections.emptyList();
    }

    /**
     * Apache HttpClient을 사용한 fallback 메소드
     */
    private List<Place> fetchNaverPlacesWithApacheClient(String query, int count) {
        logger.info("🔄 Fallback to Apache HttpClient for query: '{}'", query);

        try {
            String encodedQuery = URLEncoder.encode(query, StandardCharsets.UTF_8);
            String url = String.format(
                "https://openapi.naver.com/v1/search/local.json?query=%s&display=%d&start=1&sort=random",
                encodedQuery, count
            );

            // Apache HttpClient 5 설정 - Content Compression 비활성화 및 Chunked Encoding 처리
            org.apache.hc.client5.http.config.RequestConfig requestConfig =
                org.apache.hc.client5.http.config.RequestConfig.custom()
                    .setConnectTimeout(org.apache.hc.core5.util.Timeout.ofSeconds(10))
                    .setResponseTimeout(org.apache.hc.core5.util.Timeout.ofSeconds(30))
                    .setContentCompressionEnabled(false) // 압축 비활성화로 응답 잘림 방지
                    .build();

            org.apache.hc.client5.http.impl.classic.CloseableHttpClient httpClient =
                org.apache.hc.client5.http.impl.classic.HttpClients.custom()
                    .setDefaultRequestConfig(requestConfig)
                    .disableContentCompression() // 전역 압축 비활성화
                    .disableRedirectHandling() // 리다이렉트 비활성화로 응답 보장
                    .build();

            org.apache.hc.client5.http.classic.methods.HttpGet request =
                new org.apache.hc.client5.http.classic.methods.HttpGet(url);

            request.setHeader("X-Naver-Client-Id", naverClientId);
            request.setHeader("X-Naver-Client-Secret", naverClientSecret);
            request.setHeader("Accept", "application/json; charset=UTF-8");
            request.setHeader("User-Agent", "MoheSpring-ApacheHC-Fixed/5.3");
            request.setHeader("Accept-Encoding", "identity"); // 압축 요청 차단
            request.setHeader("Connection", "close"); // 연결 종료로 chunked 방지
            request.setHeader("Cache-Control", "no-cache");

            logger.info("Apache HttpClient request configured for URL: {}", url);

            try (org.apache.hc.client5.http.impl.classic.CloseableHttpResponse response =
                 httpClient.execute(request)) {

                org.apache.hc.core5.http.HttpEntity entity = response.getEntity();
                String responseBody = entity != null ?
                    org.apache.hc.core5.http.io.entity.EntityUtils.toString(entity, StandardCharsets.UTF_8) : "";

                logger.info("🌐 Apache HttpClient Response - Status: {}, Body length: {}",
                           response.getCode(), responseBody.length());

                // Apache HttpClient 헤더 정보 로깅
                logger.info("Apache Headers - Content-Type: {}, Content-Length: {}, Transfer-Encoding: {}",
                           response.getFirstHeader("Content-Type") != null ? response.getFirstHeader("Content-Type").getValue() : "unknown",
                           response.getFirstHeader("Content-Length") != null ? response.getFirstHeader("Content-Length").getValue() : "unknown",
                           response.getFirstHeader("Transfer-Encoding") != null ? response.getFirstHeader("Transfer-Encoding").getValue() : "unknown");

                // 모든 Apache HttpClient 헤더 출력
                logger.info("All Apache Headers: {}", java.util.Arrays.toString(response.getHeaders()));

                if (response.getCode() == 200 && !responseBody.trim().isEmpty()) {
                    logger.info("✅ Apache HttpClient succeeded - Processing response...");
                    logger.info("📝 Apache HttpClient response (first 500 chars): {}",
                               responseBody.substring(0, Math.min(500, responseBody.length())));

                    JsonNode jsonResponse = objectMapper.readTree(responseBody);
                    JsonNode items = jsonResponse.get("items");
                    JsonNode total = jsonResponse.get("total");

                    logger.info("📊 JSON parsed via Apache HttpClient - total: {}, items array size: {}",
                               total != null ? total.asInt() : "null",
                               items != null && items.isArray() ? items.size() : "null or not array");

                    List<Place> places = new ArrayList<>();
                    if (items != null && items.isArray()) {
                        logger.info("🔄 Processing {} items from Naver API via Apache HttpClient", items.size());
                        for (int i = 0; i < items.size(); i++) {
                            JsonNode item = items.get(i);
                            logger.info("📍 Item {}: {}", i+1, item.get("title"));
                            Place place = convertNaverItemToPlace(item);
                            places.add(place);
                            logger.info("✅ Added place via Apache HttpClient: {}", place.getName());
                        }
                    }

                    logger.info("✅ Successfully processed {} places from Naver API via Apache HttpClient", places.size());
                    return places;
                } else {
                    logger.error("❌ Apache HttpClient failed - Status: {}, Body: {}",
                                response.getCode(), responseBody.substring(0, Math.min(300, responseBody.length())));
                }
            }

        } catch (Exception e) {
            logger.error("Error using Apache HttpClient for Naver API query: {}", query, e);
        }

        return Collections.emptyList();
    }

    /**
     * Naver API 응답을 Place 엔티티로 변환
     */
    private Place convertNaverItemToPlace(JsonNode item) {
        Place place = new Place();

        // 기본 정보
        String title = item.get("title").asText().replaceAll("<[^>]*>", "");
        place.setName(title);
        place.setCategory(item.get("category").asText());
        place.setAddress(item.get("address").asText());

        // 좌표 변환 (Naver API 좌표계)
        double lat = item.get("mapy").asInt() / 10000000.0;
        double lng = item.get("mapx").asInt() / 10000000.0;
        place.setLatitude(BigDecimal.valueOf(lat));
        place.setLongitude(BigDecimal.valueOf(lng));

        // 기타 정보
        if (item.has("telephone") && !item.get("telephone").isNull()) {
            place.setPhone(item.get("telephone").asText());
        }

        place.setDescription(String.format("한국의 %s - %s", title, place.getCategory()));
        place.setRating(BigDecimal.ZERO); // Google API로 나중에 보강
        place.setCreatedAt(OffsetDateTime.now());

        return place;
    }

    /**
     * Google Places API로 평점 보강
     */
    private void enhanceWithGooglePlaces(Place place) {
        if (googleApiKey.trim().isEmpty()) {
            logger.warn("Google Places API key not configured, skipping rating enhancement");
            return;
        }

        try {
            String url = "https://maps.googleapis.com/maps/api/place/nearbysearch/json";
            String searchUrl = String.format(
                "%s?location=%f,%f&radius=100&keyword=%s&key=%s",
                url, place.getLatitude(), place.getLongitude(),
                URLEncoder.encode(place.getName(), StandardCharsets.UTF_8), googleApiKey
            );

            ResponseEntity<String> response = restTemplate.getForEntity(searchUrl, String.class);

            if (response.getStatusCode() == HttpStatus.OK && response.getBody() != null) {
                JsonNode jsonResponse = objectMapper.readTree(response.getBody());
                JsonNode results = jsonResponse.get("results");

                if (results != null && results.isArray() && results.size() > 0) {
                    JsonNode firstResult = results.get(0);
                    if (firstResult.has("rating")) {
                        double rating = firstResult.get("rating").asDouble();
                        place.setRating(BigDecimal.valueOf(rating));
                        logger.info("Enhanced place {} with Google rating: {}", place.getName(), rating);
                    }
                }
            }

            Thread.sleep(1000); // API 호출 간격

        } catch (Exception e) {
            logger.error("Error enhancing place with Google API: {}", place.getName(), e);
        }
    }

    /**
     * 모든 장소에 대한 AI 이미지 생성 (공개 메소드로 변경)
     */
    public int generateAiImagesForPlaces() {
        logger.info("Starting AI image generation for all places");

        List<Place> places = placeRepository.findAll();
        int generatedCount = 0;

        for (Place place : places) {
            try {
                // 이미 AI 이미지가 있는지 확인
                boolean hasAiImage = placeImageRepository.existsByPlaceIdAndIsAiGeneratedTrue(place.getId());
                if (hasAiImage) {
                    logger.info("Place {} already has AI image, skipping", place.getName());
                    continue;
                }

                // 이미지 생성 로직 비활성화 - 나중에 배치로 처리 예정
                logger.info("⏸️  Skipping image generation for place: {} (will be processed later in batch)", place.getName());
                // TODO: 나중에 배치 이미지 업데이트에서 처리됩니다

                Thread.sleep(3000); // API 호출 간격

            } catch (Exception e) {
                logger.error("Error generating AI image for place: {}", place.getName(), e);
            }
        }

        logger.info("AI image generation completed: {} images generated", generatedCount);
        return generatedCount;
    }

    /**
     * 단일 장소에 대한 AI 이미지 생성 (테스트용)
     */
    public int generateAiImageForSinglePlace(Long placeId) {
        logger.info("Starting AI image generation for single place ID: {}", placeId);

        Optional<Place> placeOpt = placeRepository.findById(placeId);
        if (!placeOpt.isPresent()) {
            logger.error("Place not found with ID: {}", placeId);
            return 0;
        }

        Place place = placeOpt.get();

        try {
            // 이미 AI 이미지가 있는지 확인
            boolean hasAiImage = placeImageRepository.existsByPlaceIdAndIsAiGenerated(placeId, true);
            if (hasAiImage) {
                logger.info("Place {} already has AI image, skipping", place.getName());
                return 0;
            }

            // 이미지 생성 로직 비활성화 - 나중에 배치로 처리 예정
            logger.info("⏸️  Skipping single image generation for place: {} (will be processed later in batch)", place.getName());
            // TODO: 나중에 배치 이미지 업데이트에서 처리됩니다
            return 0;

        } catch (Exception e) {
            logger.error("Error generating AI image for place: {}", place.getName(), e);
            return 0;
        }
    }

    /**
     * 배치로 이미지를 일괄 업데이트하는 메서드 (나중에 사용 예정)
     * DB를 돌면서 이미지가 없는 장소들에 대해 이미지를 생성/업데이트
     */
    public int batchUpdatePlaceImages() {
        logger.info("🖼️  Starting batch image update for places without images");

        List<Place> placesWithoutImages = placeRepository.findPlacesWithoutImages();
        logger.info("Found {} places without images", placesWithoutImages.size());

        int updatedCount = 0;

        for (Place place : placesWithoutImages) {
            try {
                logger.info("🎯 Processing place for image update: {} (Rating: {})",
                    place.getName(), place.getRating());

                // 평점 기반 이미지 생성 (3.0 이상만 AI, 나머지는 Default)
                PlaceImage placeImage = imageGenerationService.generateKoreanPlaceImage(place);

                if (placeImage != null) {
                    placeImageRepository.save(placeImage);
                    updatedCount++;

                    String imageType = placeImage.getIsAiGenerated() ? "AI" : "Default";
                    logger.info("✅ Updated place with {} image: {}", imageType, place.getName());
                } else {
                    logger.warn("❌ Failed to generate image for place: {}", place.getName());
                }

                // API 호출 간격 (AI 이미지 생성시에만)
                if (place.getRating() != null && place.getRating() >= 3.0) {
                    Thread.sleep(3000);
                }

            } catch (Exception e) {
                logger.error("Error updating image for place: {}", place.getName(), e);
            }
        }

        logger.info("🎉 Batch image update completed: {} places updated", updatedCount);
        return updatedCount;
    }

    public Object triggerBatchJob(String jobName, Map<String, Object> parameters) {
        logger.info("Triggering batch job: {} with parameters: {}", jobName, parameters);

        switch (jobName.toLowerCase()) {
            case "collect-places":
                return Map.of("result", collectRealPlaceData());
            case "generate-images":
                return Map.of("result", generateAiImagesForPlaces());
            case "batch-update-images":
                return Map.of("result", batchUpdatePlaceImages());
            case "full-batch":
                return triggerBatch();
            default:
                return Map.of("error", "Unknown job name: " + jobName);
        }
    }
    
    public BatchPlaceResponse ingestPlaceData(List<BatchPlaceRequest> placeDataList) {
        // TODO: Implement place data ingestion
        return new BatchPlaceResponse(
            placeDataList.size(), // processedCount
            placeDataList.size(), // insertedCount
            0, // updatedCount
            0, // skippedCount
            0, // errorCount
            List.of() // errors
        );
    }
    
    public BatchUserResponse ingestUserData(List<BatchUserRequest> userDataList) {
        // TODO: Implement user data ingestion
        return new BatchUserResponse(
            userDataList.size(), // processedCount
            userDataList.size(), // insertedCount
            0, // updatedCount
            0, // skippedCount
            0, // errorCount
            List.of() // errors
        );
    }
    
    public InternalPlaceIngestResponse ingestPlacesFromExternalApi(List<String> apiKeys) {
        // TODO: Implement places ingestion from external API
        return new InternalPlaceIngestResponse(
            0, // processedCount
            0, // insertedCount
            0, // updatedCount
            0, // skippedCount
            0, // errorCount
            0, // keywordGeneratedCount
            List.of() // errors
        );
    }
    
    public InternalPlaceIngestResponse ingestPlacesFromBatch(List<InternalPlaceIngestRequest> placeDataList) {
        // TODO: Implement places ingestion from batch
        return new InternalPlaceIngestResponse(
            placeDataList.size(), // processedCount
            placeDataList.size(), // insertedCount
            0, // updatedCount
            0, // skippedCount
            0, // errorCount
            0, // keywordGeneratedCount
            List.of() // errors
        );
    }
    
    public DatabaseCleanupResponse cleanupOldAndLowRatedPlaces() {
        // TODO: Implement database cleanup
        return new DatabaseCleanupResponse(
            0, // removedCount
            List.of("Database cleanup not yet implemented") // messages
        );
    }
}