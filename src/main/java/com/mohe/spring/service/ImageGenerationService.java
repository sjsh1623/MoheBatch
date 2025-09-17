package com.mohe.spring.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mohe.spring.entity.ImageSource;
import com.mohe.spring.entity.Place;
import com.mohe.spring.entity.PlaceImage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.*;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.time.OffsetDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

@Service
public class ImageGenerationService {

    private static final Logger logger = LoggerFactory.getLogger(ImageGenerationService.class);

    private final RestTemplate restTemplate;
    private final ObjectMapper objectMapper;

    @Value("${OPENAI_API_KEY:}")
    private String openaiApiKey;

    @Value("${IMAGE_STORAGE_DIR:/host/images}")
    private String imageStorageDir;

    public ImageGenerationService() {
        this.restTemplate = new RestTemplate();
        this.objectMapper = new ObjectMapper();
    }

    /**
     * 평점 기반 이미지 생성 (3점 이상만 AI 생성, 나머지는 Default)
     */
    public PlaceImage generateKoreanPlaceImage(Place place) {
        try {
            // 평점 확인 (3.0 이상만 AI 이미지 생성)
            Double rating = place.getRating();
            boolean shouldGenerateAI = rating != null && rating >= 3.0;

            if (shouldGenerateAI) {
                return generateAIImage(place);
            } else {
                return generateDefaultImage(place);
            }

        } catch (Exception e) {
            logger.error("Error generating image for place: {}", place.getName(), e);
            return generateDefaultImage(place); // 오류 시 기본 이미지
        }
    }

    /**
     * AI 이미지 생성 (평점 3.0 이상)
     */
    private PlaceImage generateAIImage(Place place) {
        if (openaiApiKey == null || openaiApiKey.trim().isEmpty()) {
            logger.error("OpenAI API key is not configured, using default image");
            return generateDefaultImage(place);
        }

        try {
            // 개선된 프롬프트 생성
            String prompt = generateKoreanPlacePrompt(place);
            logger.info("Generating AI image for high-rated place ({}★): {} with prompt: {}",
                place.getRating(), place.getName(), prompt.substring(0, Math.min(100, prompt.length())) + "...");

            // OpenAI API 호출
            String imageUrl = callOpenAIImageGeneration(prompt);
            if (imageUrl == null) {
                logger.error("Failed to generate image from OpenAI API, using default image");
                return generateDefaultImage(place);
            }

            // 이미지 다운로드 및 저장
            String localImagePath = downloadAndSaveImage(imageUrl, place.getName());
            if (localImagePath == null) {
                logger.error("Failed to download and save image, using default image");
                return generateDefaultImage(place);
            }

            // PlaceImage 엔티티 생성
            PlaceImage placeImage = new PlaceImage();
            placeImage.setPlace(place);
            placeImage.setImageUrl(localImagePath);  // 웹 접근 경로
            placeImage.setImagePath(localImagePath); // 파일 시스템 경로
            placeImage.setSource(ImageSource.AI_GENERATED);
            placeImage.setIsAiGenerated(true);
            placeImage.setAiModel("dall-e-3");
            placeImage.setPromptUsed(prompt);
            placeImage.setIsPrimary(true);
            placeImage.setIsVerified(false);
            placeImage.setCreatedAt(OffsetDateTime.now());
            placeImage.setUpdatedAt(OffsetDateTime.now());

            logger.info("✅ Successfully generated AI image for place: {} (rating: {})", place.getName(), place.getRating());
            return placeImage;

        } catch (Exception e) {
            logger.error("Error generating AI image for place: {}, using default image", place.getName(), e);
            return generateDefaultImage(place);
        }
    }

    /**
     * Default 이미지 생성 (평점 3.0 미만 또는 AI 생성 실패)
     */
    private PlaceImage generateDefaultImage(Place place) {
        try {
            String category = place.getCategory() != null ? place.getCategory() : "카페";
            String defaultImagePath = getDefaultImagePath(category);

            Double rating = place.getRating();
            String ratingInfo = rating != null ? String.format("%.1f", rating) : "N/A";

            logger.info("🔄 Using default image for place: {} (rating: {}★) -> {}",
                place.getName(), ratingInfo, defaultImagePath);

            // PlaceImage 엔티티 생성
            PlaceImage placeImage = new PlaceImage();
            placeImage.setPlace(place);
            placeImage.setImageUrl(defaultImagePath);  // 웹 접근 경로
            placeImage.setImagePath(defaultImagePath); // 파일 시스템 경로
            placeImage.setSource(ImageSource.MANUAL_UPLOAD);
            placeImage.setIsAiGenerated(false);
            placeImage.setAiModel(null);
            placeImage.setPromptUsed("Default image for rating < 3.0");
            placeImage.setIsPrimary(true);
            placeImage.setIsVerified(true);
            placeImage.setCreatedAt(OffsetDateTime.now());
            placeImage.setUpdatedAt(OffsetDateTime.now());

            return placeImage;

        } catch (Exception e) {
            logger.error("Error generating default image for place: {}", place.getName(), e);
            return null;
        }
    }

    /**
     * 카테고리별 기본 이미지 경로 반환
     */
    private String getDefaultImagePath(String category) {
        Map<String, String> defaultImages = new HashMap<>();
        defaultImages.put("음식점>카페,디저트", "/images/defaults/cafe_default.jpg");
        defaultImages.put("음식점>한식", "/images/defaults/korean_default.jpg");
        defaultImages.put("음식점>중식", "/images/defaults/chinese_default.jpg");
        defaultImages.put("음식점>일식", "/images/defaults/japanese_default.jpg");
        defaultImages.put("음식점>양식", "/images/defaults/western_default.jpg");
        defaultImages.put("음식점>분식", "/images/defaults/snack_default.jpg");

        return defaultImages.getOrDefault(category, "/images/defaults/general_default.jpg");
    }

    /**
     * 장소별 특성을 반영한 개선된 프롬프트 생성
     */
    private String generateKoreanPlacePrompt(Place place) {
        String cleanName = place.getName().replaceAll("<[^>]*>", "");
        String category = place.getCategory() != null ? place.getCategory() : "카페";
        String description = place.getDescription() != null ? place.getDescription() : "";

        // 카테고리별 상세 프롬프트 매핑
        Map<String, String> categoryPrompts = new HashMap<>();

        // 카페/디저트
        categoryPrompts.put("음식점>카페,디저트",
            "A realistic interior shot of a modern coffee shop. Beautiful coffee cups, pastries, desserts on display. " +
            "Cozy seating areas with comfortable chairs and tables. Coffee brewing equipment visible. " +
            "Natural lighting through large windows. Clean, minimalist design with warm atmosphere.");

        // 한식당
        categoryPrompts.put("음식점>한식",
            "A realistic interior of a Korean restaurant. Traditional Korean dishes beautifully plated on tables. " +
            "Kimchi, bulgogi, bibimbap, grilled meat visible. Traditional Korean table settings. " +
            "Comfortable dining atmosphere with modern Korean interior design.");

        // 중식당
        categoryPrompts.put("음식점>중식",
            "A realistic Chinese restaurant interior. Delicious Chinese dishes on tables - dumplings, noodles, stir-fry. " +
            "Round dining tables, elegant Chinese-style decor. Red and gold color accents. " +
            "Professional restaurant lighting and atmosphere.");

        // 일식당
        categoryPrompts.put("음식점>일식",
            "A realistic Japanese restaurant interior. Fresh sushi, sashimi, ramen bowls on display. " +
            "Clean wooden tables, minimalist Japanese design. Sushi counter with fresh fish visible. " +
            "Zen-like atmosphere with natural materials.");

        // 양식당
        categoryPrompts.put("음식점>양식",
            "A realistic Western restaurant interior. Steaks, pasta, salads beautifully plated. " +
            "Elegant dining tables with wine glasses. Modern Western decor. " +
            "Sophisticated restaurant atmosphere with professional lighting.");

        // 분식집
        categoryPrompts.put("음식점>분식",
            "A realistic Korean street food restaurant interior. Tteokbokki, kimbap, hotdogs, fried foods on display. " +
            "Casual seating areas, vibrant and energetic atmosphere. " +
            "Food preparation area visible with fresh ingredients.");

        // 기본 카페 프롬프트
        String defaultPrompt =
            "A realistic interior shot of a modern coffee shop. Beautiful coffee cups, light meals on display. " +
            "Cozy seating areas with comfortable furniture. Natural lighting. " +
            "Clean, contemporary design with welcoming atmosphere.";

        String basePrompt = categoryPrompts.getOrDefault(category, defaultPrompt);

        // Description 기반 추가 요소
        String additionalElements = "";
        if (description.contains("파스타") || description.contains("이탈리안")) {
            additionalElements += " Fresh pasta dishes and Italian cuisine visible.";
        } else if (description.contains("피자")) {
            additionalElements += " Wood-fired pizza oven and fresh pizzas visible.";
        } else if (description.contains("치킨") || description.contains("프라이드")) {
            additionalElements += " Crispy fried chicken and side dishes on display.";
        } else if (description.contains("베이커리") || description.contains("빵")) {
            additionalElements += " Fresh bread, pastries, and baked goods in display cases.";
        } else if (description.contains("바") || description.contains("칵테일")) {
            additionalElements += " Professional bar setup with bottles and cocktail glasses.";
        }

        return String.format(
            "%s%s " +
            "Professional photography, high-quality interior shot, realistic lighting. " +
            "No text, no signs, no writing visible. Focus on the food and interior atmosphere. " +
            "Modern contemporary style without traditional cultural elements.",
            basePrompt, additionalElements
        );
    }

    /**
     * OpenAI DALL-E API 호출
     */
    private String callOpenAIImageGeneration(String prompt) {
        try {
            String url = "https://api.openai.com/v1/images/generations";

            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON);
            headers.setBearerAuth(openaiApiKey);

            Map<String, Object> requestBody = new HashMap<>();
            requestBody.put("model", "dall-e-3");
            requestBody.put("prompt", prompt);
            requestBody.put("size", "1024x1024");
            requestBody.put("quality", "standard");
            requestBody.put("n", 1);

            HttpEntity<Map<String, Object>> request = new HttpEntity<>(requestBody, headers);

            ResponseEntity<String> response = restTemplate.exchange(
                url, HttpMethod.POST, request, String.class
            );

            if (response.getStatusCode() == HttpStatus.OK && response.getBody() != null) {
                JsonNode jsonResponse = objectMapper.readTree(response.getBody());
                JsonNode dataArray = jsonResponse.get("data");

                if (dataArray != null && dataArray.isArray() && dataArray.size() > 0) {
                    JsonNode firstImage = dataArray.get(0);
                    JsonNode urlNode = firstImage.get("url");
                    if (urlNode != null) {
                        return urlNode.asText();
                    }
                }
            }

            logger.error("Failed to get image URL from OpenAI response: {}", response.getBody());
            return null;

        } catch (Exception e) {
            logger.error("Error calling OpenAI image generation API", e);
            return null;
        }
    }

    /**
     * 이미지 다운로드 및 로컬 저장 (개선된 HttpURLConnection 방식)
     * 웹 검색 결과 적용: SAS 토큰 서명 보호를 위한 직접 URL 처리
     */
    private String downloadAndSaveImage(String imageUrl, String placeName) {
        try {
            logger.info("🔽 Starting image download for: {} from URL: {}", placeName, imageUrl.substring(0, Math.min(100, imageUrl.length())) + "...");

            // 안전한 파일명 생성
            String safeName = placeName.replaceAll("[^a-zA-Z0-9가-힣\\s\\-_]", "")
                                       .replaceAll("\\s+", "_")
                                       .trim();
            if (safeName.length() > 50) {
                safeName = safeName.substring(0, 50);
            }

            String fileName = safeName + "_" + UUID.randomUUID().toString().substring(0, 8) + "_ai.jpg";

            // 저장 디렉토리 확인 및 생성
            Path storageDir = Paths.get(imageStorageDir);
            if (!Files.exists(storageDir)) {
                Files.createDirectories(storageDir);
                logger.info("Created image storage directory: {}", storageDir);
            }

            // 개선된 이미지 다운로드: HttpURLConnection 사용하여 SAS 토큰 서명 보호
            byte[] imageData = downloadImageWithHttpURLConnection(imageUrl);
            if (imageData == null || imageData.length == 0) {
                logger.error("Failed to download image from URL: {}", imageUrl);
                return null;
            }

            // 파일 저장
            Path filePath = storageDir.resolve(fileName);
            Files.write(filePath, imageData);

            // 웹 접근 가능한 경로 반환
            String webPath = "/images/places/" + fileName;
            logger.info("✅ Image saved successfully: {} -> {} ({} bytes)", placeName, webPath, imageData.length);

            return webPath;

        } catch (Exception e) {
            logger.error("❌ Error downloading and saving image from URL: {}", imageUrl, e);
            return null;
        }
    }

    /**
     * HttpURLConnection을 사용한 이미지 다운로드
     * 웹 검색 결과: SAS 토큰 URL을 직접 사용하여 서명 손상 방지
     */
    private byte[] downloadImageWithHttpURLConnection(String imageUrl) throws Exception {
        logger.info("🌐 Using HttpURLConnection for SAS URL download");

        // 웹 검색 결과: SAS URL을 직접 사용하여 서명 보호
        java.net.URL url = new java.net.URL(imageUrl);
        java.net.HttpURLConnection connection = (java.net.HttpURLConnection) url.openConnection();

        try {
            // 연결 설정
            connection.setRequestMethod("GET");
            connection.setConnectTimeout(15000); // 15초
            connection.setReadTimeout(30000);    // 30초

            // Azure Blob Storage용 헤더 설정
            connection.setRequestProperty("User-Agent", "MoheSpring-ImageDownloader/1.0");
            connection.setRequestProperty("Accept", "*/*");

            // 웹 검색 결과: 추가 인코딩 방지
            connection.setUseCaches(false);
            connection.setInstanceFollowRedirects(true);

            int responseCode = connection.getResponseCode();
            logger.info("📊 Image download response code: {}", responseCode);

            if (responseCode == 200) {
                // 성공적인 응답에서 이미지 데이터 읽기
                try (java.io.InputStream inputStream = connection.getInputStream();
                     java.io.ByteArrayOutputStream outputStream = new java.io.ByteArrayOutputStream()) {

                    byte[] buffer = new byte[8192];
                    int bytesRead;
                    int totalBytes = 0;

                    while ((bytesRead = inputStream.read(buffer)) != -1) {
                        outputStream.write(buffer, 0, bytesRead);
                        totalBytes += bytesRead;
                    }

                    logger.info("📥 Downloaded {} bytes successfully", totalBytes);
                    return outputStream.toByteArray();
                }
            } else {
                // 오류 응답 로깅
                try (java.io.InputStream errorStream = connection.getErrorStream()) {
                    if (errorStream != null) {
                        String errorResponse = new String(errorStream.readAllBytes(), StandardCharsets.UTF_8);
                        logger.error("🚫 HTTP {} Error Response: {}", responseCode, errorResponse);
                    }
                }
                throw new RuntimeException("HTTP Error " + responseCode + " when downloading image");
            }

        } finally {
            connection.disconnect();
        }
    }
}