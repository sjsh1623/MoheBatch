package com.mohe.spring.controller;

import com.mohe.spring.service.BatchService;
import com.mohe.spring.service.ImageGenerationService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;
import java.util.HashMap;

@RestController
@RequestMapping("/test")
public class TestController {

    @Autowired
    private BatchService batchService;

    @GetMapping("/collect-places")
    public ResponseEntity<Map<String, Object>> testCollectPlaces() {
        try {
            // BatchService의 새로운 HTTP 클라이언트 메소드 테스트
            int collected = batchService.collectRealPlaceData();
            return ResponseEntity.ok(Map.of(
                "success", true,
                "collected", collected,
                "message", "Place collection completed"
            ));
        } catch (Exception e) {
            return ResponseEntity.internalServerError().body(Map.of(
                "success", false,
                "error", e.getMessage(),
                "stackTrace", java.util.Arrays.toString(e.getStackTrace()),
                "message", "Place collection failed"
            ));
        }
    }


    @GetMapping("/test-simple")
    public ResponseEntity<Map<String, Object>> testSimpleRestTemplate() {
        try {
            // Web Search에서 발견한 107자 잘림 해결 방법: 적절한 RestTemplate 설정
            String uri = org.springframework.web.util.UriComponentsBuilder
                .fromUriString("https://openapi.naver.com")
                .path("/v1/search/local.json")
                .queryParam("query", "홍대카페")
                .queryParam("display", 3)
                .queryParam("start", 1)
                .queryParam("sort", "random")
                .toUriString();

            // 107자 잘림 해결: 적절한 HTTP client factory 설정
            org.springframework.http.client.SimpleClientHttpRequestFactory factory =
                new org.springframework.http.client.SimpleClientHttpRequestFactory();
            factory.setConnectTimeout(10000);
            factory.setReadTimeout(30000);
            factory.setBufferRequestBody(false); // 중요: 요청 버퍼링 비활성화

            org.springframework.web.client.RestTemplate restTemplate = new org.springframework.web.client.RestTemplate(factory);

            // 107자 잘림 해결: 적절한 message converter 설정
            restTemplate.getMessageConverters().clear();
            restTemplate.getMessageConverters().add(
                new org.springframework.http.converter.StringHttpMessageConverter(java.nio.charset.StandardCharsets.UTF_8)
            );
            restTemplate.getMessageConverters().add(
                new org.springframework.http.converter.json.MappingJackson2HttpMessageConverter()
            );

            org.springframework.http.RequestEntity<Void> req = org.springframework.http.RequestEntity
                .get(uri)
                .header("X-Naver-Client-Id", "tKZJapF56F_c3hUdXqJj")
                .header("X-Naver-Client-Secret", "yjY57FTiJ3")
                .header("Accept", "application/json; charset=UTF-8")
                .header("User-Agent", "MoheSpring-Fixed/1.0") // User-Agent 명시
                .build();

            org.springframework.http.ResponseEntity<String> responseEntity = restTemplate.exchange(req, String.class);

            String responseBody = responseEntity.getBody();
            int bodyLength = responseBody != null ? responseBody.length() : 0;

            return ResponseEntity.ok(Map.of(
                "success", true,
                "method", "Fixed RestTemplate (107자 잘림 해결)",
                "status", responseEntity.getStatusCode().toString(),
                "bodyLength", bodyLength,
                "preview", responseBody != null ? responseBody.substring(0, Math.min(300, bodyLength)) : "",
                "headers", responseEntity.getHeaders().toSingleValueMap()
            ));
        } catch (Exception e) {
            e.printStackTrace();
            return ResponseEntity.internalServerError().body(Map.of(
                "success", false,
                "error", e.getMessage(),
                "message", "Fixed RestTemplate test failed",
                "errorType", e.getClass().getSimpleName()
            ));
        }
    }

    @GetMapping("/test-java-client")
    public ResponseEntity<Map<String, Object>> testJavaHttpClient() {
        try {
            String result = batchService.testJavaHttpClient();
            return ResponseEntity.ok(Map.of(
                "success", true,
                "result", result,
                "message", "Java HTTP Client test completed"
            ));
        } catch (Exception e) {
            return ResponseEntity.internalServerError().body(Map.of(
                "success", false,
                "error", e.getMessage(),
                "message", "Java HTTP Client test failed"
            ));
        }
    }

    @GetMapping("/generate-images")
    public ResponseEntity<Map<String, Object>> testGenerateImages() {
        try {
            int generated = batchService.generateAiImagesForPlaces();
            return ResponseEntity.ok(Map.of(
                "success", true,
                "generated", generated,
                "message", "Image generation completed"
            ));
        } catch (Exception e) {
            return ResponseEntity.internalServerError().body(Map.of(
                "success", false,
                "error", e.getMessage(),
                "message", "Image generation failed"
            ));
        }
    }

    @GetMapping("/status")
    public ResponseEntity<Map<String, Object>> getStatus() {
        try {
            Map<String, Object> status = batchService.getBatchStatus();
            return ResponseEntity.ok(status);
        } catch (Exception e) {
            return ResponseEntity.internalServerError().body(Map.of(
                "success", false,
                "error", e.getMessage()
            ));
        }
    }

    @GetMapping("/generate-single-image")
    public ResponseEntity<Map<String, Object>> testGenerateSingleImage(@RequestParam(defaultValue = "57") Long placeId) {
        try {
            int generated = batchService.generateAiImageForSinglePlace(placeId);
            return ResponseEntity.ok(Map.of(
                "success", true,
                "generated", generated,
                "placeId", placeId,
                "message", "Single image generation completed"
            ));
        } catch (Exception e) {
            return ResponseEntity.internalServerError().body(Map.of(
                "success", false,
                "error", e.getMessage(),
                "placeId", placeId,
                "message", "Single image generation failed"
            ));
        }
    }

    @GetMapping("/create-placeholder-images")
    public ResponseEntity<Map<String, Object>> createPlaceholderImages() {
        try {
            int created = batchService.createPlaceholderImages();
            return ResponseEntity.ok(Map.of(
                "success", true,
                "created", created,
                "message", "Placeholder image creation completed"
            ));
        } catch (Exception e) {
            return ResponseEntity.internalServerError().body(Map.of(
                "success", false,
                "error", e.getMessage(),
                "message", "Placeholder image creation failed"
            ));
        }
    }

    @GetMapping("/batch-update-images")
    public ResponseEntity<Map<String, Object>> batchUpdateImages() {
        try {
            int updated = batchService.batchUpdatePlaceImages();
            return ResponseEntity.ok(Map.of(
                "success", true,
                "updated", updated,
                "message", "Batch image update completed"
            ));
        } catch (Exception e) {
            return ResponseEntity.internalServerError().body(Map.of(
                "success", false,
                "error", e.getMessage(),
                "message", "Batch image update failed"
            ));
        }
    }

    @GetMapping("/test-category-mapping")
    public ResponseEntity<Map<String, Object>> testCategoryMapping(@RequestParam(required = false) String category) {
        try {
            if (category == null) {
                // 현재 데이터베이스의 모든 카테고리 매핑 테스트
                Map<String, String> allMappings = new HashMap<>();

                // 샘플 카테고리들로 테스트
                String[] testCategories = {
                    "카페,디저트>와플", "음식점>카페,디저트", "한식>조개요리",
                    "술집>바(BAR)", "갤러리카페", "음식점>양식", "음식점>햄버거"
                };

                for (String testCategory : testCategories) {
                    ImageGenerationService imageService = new ImageGenerationService();
                    String defaultPath = imageService.getDefaultImagePath(testCategory);
                    allMappings.put(testCategory, defaultPath);
                }

                return ResponseEntity.ok(Map.of(
                    "success", true,
                    "mappings", allMappings,
                    "message", "Category mapping test completed"
                ));
            } else {
                // 특정 카테고리 테스트
                ImageGenerationService imageService = new ImageGenerationService();
                String defaultPath = imageService.getDefaultImagePath(category);

                return ResponseEntity.ok(Map.of(
                    "success", true,
                    "category", category,
                    "defaultPath", defaultPath,
                    "message", "Single category mapping test completed"
                ));
            }
        } catch (Exception e) {
            return ResponseEntity.internalServerError().body(Map.of(
                "success", false,
                "error", e.getMessage(),
                "message", "Category mapping test failed"
            ));
        }
    }
}