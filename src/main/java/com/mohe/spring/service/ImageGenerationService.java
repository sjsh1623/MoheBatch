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

    @Value("${GEMINI_API_KEY:}")
    private String geminiApiKey;

    @Value("${IMAGE_STORAGE_DIR:/host/images}")
    private String imageStorageDir;

    @Value("${IMAGE_SERVER_BASE_URL:http://localhost:1000}")
    private String imageServerBaseUrl;

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
            java.math.BigDecimal rating = place.getRating();
            boolean shouldGenerateAI = rating != null && rating.compareTo(java.math.BigDecimal.valueOf(3.0)) >= 0;

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

            java.math.BigDecimal rating = place.getRating();
            String ratingInfo = rating != null ? String.format("%.1f", rating.doubleValue()) : "N/A";

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
     * 카테고리별 기본 이미지 경로 반환 (URL 슬러그 기반) - Public 메서드
     */
    public String getDefaultImagePath(String category) {
        // 1단계: 카테고리를 표준화된 영어 카테고리로 매핑
        String standardCategory = mapToStandardCategory(category);

        // 2단계: 표준 카테고리를 URL 슬러그로 변환
        String slug = convertToSlug(standardCategory);

        // 3단계: 슬러그를 기본 이미지 URL로 매핑
        Map<String, String> defaultImageMap = getDefaultImageMap();

        return defaultImageMap.getOrDefault(slug, "/default.jpg");
    }

    /**
     * 한국어/기타 카테고리를 표준 영어 카테고리로 매핑
     */
    private String mapToStandardCategory(String category) {
        if (category == null) return "default";

        String lowerCategory = category.toLowerCase();

        // 한국어 카테고리 매핑
        if (lowerCategory.contains("카페") || lowerCategory.contains("cafe") || lowerCategory.contains("디저트")) {
            return "Cafe";
        } else if (lowerCategory.contains("한식") || lowerCategory.contains("korean")) {
            return "Korean Restaurant";
        } else if (lowerCategory.contains("중식") || lowerCategory.contains("chinese")) {
            return "Chinese Restaurant";
        } else if (lowerCategory.contains("일식") || lowerCategory.contains("japanese") || lowerCategory.contains("sushi")) {
            return "Japanese Restaurant";
        } else if (lowerCategory.contains("양식") || lowerCategory.contains("western") || lowerCategory.contains("pasta") || lowerCategory.contains("steak")) {
            return "Western Restaurant";
        } else if (lowerCategory.contains("바") || lowerCategory.contains("bar") || lowerCategory.contains("칵테일")) {
            return "Bar";
        } else if (lowerCategory.contains("분식") || lowerCategory.contains("fast") || lowerCategory.contains("burger")) {
            return "Fast Food";
        } else if (lowerCategory.contains("베이커리") || lowerCategory.contains("bakery") || lowerCategory.contains("빵")) {
            return "Bakery";
        } else if (lowerCategory.contains("해산물") || lowerCategory.contains("seafood") || lowerCategory.contains("조개")) {
            return "Seafood Restaurant";
        } else if (lowerCategory.contains("궁") || lowerCategory.contains("palace")) {
            return "Palace";
        } else if (lowerCategory.contains("박물관") || lowerCategory.contains("museum")) {
            return "Museum";
        } else if (lowerCategory.contains("갤러리") || lowerCategory.contains("gallery")) {
            return "Art Gallery";
        } else if (lowerCategory.contains("공원") || lowerCategory.contains("park")) {
            return "Park";
        } else if (lowerCategory.contains("호텔") || lowerCategory.contains("hotel")) {
            return "Hotel";
        } else if (lowerCategory.contains("스파") || lowerCategory.contains("spa")) {
            return "Spa";
        } else if (lowerCategory.contains("클럽") || lowerCategory.contains("club")) {
            return "Club";
        } else if (lowerCategory.contains("시장") || lowerCategory.contains("market")) {
            return "Market";
        } else if (lowerCategory.contains("쇼핑") || lowerCategory.contains("mall")) {
            return "Shopping Mall";
        } else if (lowerCategory.contains("도서관") || lowerCategory.contains("library")) {
            return "Library";
        } else if (lowerCategory.contains("극장") || lowerCategory.contains("theater")) {
            return "Theater";
        } else if (lowerCategory.contains("테마파크") || lowerCategory.contains("theme")) {
            return "Theme Park";
        } else if (lowerCategory.contains("체험") || lowerCategory.contains("experience")) {
            return "Experience Space";
        }

        return "default";
    }

    /**
     * 표준 카테고리를 URL 슬러그로 변환
     */
    private String convertToSlug(String category) {
        if (category == null || category.equals("default")) return "default";

        return category.toLowerCase()
                .replaceAll("[^a-z0-9\\s]", "") // 특수문자 제거
                .replaceAll("\\s+", "-") // 공백을 하이픈으로
                .trim();
    }

    /**
     * 슬러그별 기본 이미지 URL 매핑
     */
    private Map<String, String> getDefaultImageMap() {
        Map<String, String> imageMap = new HashMap<>();

        // 사용자 요청에 따른 매핑 (null 절대 사용 안함)
        imageMap.put("cafe", "/cafe.jpg");
        imageMap.put("palace", "/palace.jpg");
        imageMap.put("bar", "/bar.jpg");
        imageMap.put("korean-restaurant", "/korean-restaurant.jpg");
        imageMap.put("western-restaurant", "/western-restaurant.jpg");
        imageMap.put("chinese-restaurant", "/chinese-restaurant.jpg");
        imageMap.put("museum", "/museum.jpg");
        imageMap.put("theme-park", "/theme-park.jpg");
        imageMap.put("art-gallery", "/art-gallery.jpg");
        imageMap.put("theater", "/theater.jpg");
        imageMap.put("market", "/market.jpg");
        imageMap.put("shopping-mall", "/shopping-mall.jpg");
        imageMap.put("park", "/park.jpg");
        imageMap.put("library", "/library.jpg");
        imageMap.put("hotel", "/hotel.jpg");
        imageMap.put("spa", "/spa.jpg");
        imageMap.put("club", "/club.jpg");
        imageMap.put("bakery", "/bakery.jpg");
        imageMap.put("fast-food", "/fast-food.jpg");
        imageMap.put("japanese-restaurant", "/japanese-restaurant.jpg");
        imageMap.put("seafood-restaurant", "/seafood-restaurant.jpg");
        imageMap.put("experience-space", "/experience-space.jpg");
        imageMap.put("default", "/default.jpg");

        return imageMap;
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
        return downloadAndSaveImageToFolder(imageUrl, placeName, "place", "_ai");
    }

    /**
     * 메뉴 이미지 다운로드 및 저장
     * 폴더: images/menu/
     * 파일명: {placeId}_{menuName}_{uuid}.jpg
     */
    public String downloadAndSaveMenuImage(String imageUrl, Long placeId, String menuName) {
        if (imageUrl == null || imageUrl.isEmpty() || imageUrl.equals("https://search.pstatic.net/common/")) {
            logger.debug("Skipping invalid menu image URL: {}", imageUrl);
            return null;
        }

        String safeName = String.format("%d_%s", placeId, sanitizeFileName(menuName));
        return downloadAndSaveImageToFolder(imageUrl, safeName, "menu", "");
    }

    /**
     * 장소 이미지 다운로드 및 저장 (크롤링용)
     * 폴더: images/place/
     * 파일명: {placeName}_{uuid}.jpg
     */
    public String downloadAndSavePlaceImage(String imageUrl, String placeName) {
        if (imageUrl == null || imageUrl.isEmpty()) {
            logger.debug("Skipping invalid place image URL");
            return null;
        }

        String safeName = sanitizeFileName(placeName);
        return downloadAndSaveImageToFolder(imageUrl, safeName, "place", "");
    }

    /**
     * 파일명 안전화 처리
     */
    private String sanitizeFileName(String name) {
        if (name == null) return "unknown";

        String safeName = name.replaceAll("[^a-zA-Z0-9가-힣\\s\\-_]", "")
                             .replaceAll("\\s+", "_")
                             .trim();
        if (safeName.length() > 50) {
            safeName = safeName.substring(0, 50);
        }
        return safeName.isEmpty() ? "unknown" : safeName;
    }

    /**
     * 이미지를 지정된 폴더에 다운로드 및 저장
     * @param imageUrl 이미지 URL
     * @param baseName 파일 기본 이름
     * @param subfolder 하위 폴더 (place 또는 menu)
     * @param suffix 파일명 접미사 (예: _ai)
     * @return 웹 접근 가능 경로 또는 null
     */
    private String downloadAndSaveImageToFolder(String imageUrl, String baseName, String subfolder, String suffix) {
        try {
            logger.info("🔽 Starting image download to {}: {} from URL: {}",
                subfolder, baseName, imageUrl.substring(0, Math.min(100, imageUrl.length())) + "...");

            String fileName = baseName + "_" + UUID.randomUUID().toString().substring(0, 8) + suffix + ".jpg";

            // 저장 디렉토리 확인 및 생성 (images/place 또는 images/menu)
            Path storageDir = Paths.get(imageStorageDir, subfolder);
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
            String webPath = "/images/" + subfolder + "/" + fileName;
            logger.info("✅ Image saved successfully: {} -> {} ({} bytes)", baseName, webPath, imageData.length);

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

    /**
     * Gemini API로 장소 이미지 생성 (사용자 요구사항)
     */
    public String generatePlaceImage(Place place, String imagePrompt) {
        try {
            if (geminiApiKey == null || geminiApiKey.isEmpty()) {
                logger.warn("Gemini API 키가 없어서 기본 이미지 사용: {}", place.getName());
                return getDefaultImagePath(place.getCategory());
            }

            logger.info("🎨 Gemini API로 이미지 생성 시작: {}", place.getName());

            // Gemini API 호출
            String imageUrl = callGeminiImageAPI(imagePrompt, place.getName());

            if (imageUrl != null) {
                // 이미지 다운로드 및 로컬 저장
                String localPath = downloadAndSaveGeminiImage(imageUrl, place.getName());

                if (localPath != null) {
                    // localhost:1000으로 접근 가능한 URL 반환
                    String accessUrl = imageServerBaseUrl + localPath;
                    logger.info("✅ Gemini 이미지 생성 완료: {} -> {}", place.getName(), accessUrl);
                    return accessUrl;
                }
            }

            // 실패시 기본 이미지 반환
            logger.warn("⚠️ Gemini 이미지 생성 실패, 기본 이미지 사용: {}", place.getName());
            return getDefaultImagePath(place.getCategory());

        } catch (Exception e) {
            logger.error("❌ Gemini 이미지 생성 중 오류 for {}: {}", place.getName(), e.getMessage());
            return getDefaultImagePath(place.getCategory());
        }
    }

    /**
     * Gemini Image Generation API 호출
     */
    private String callGeminiImageAPI(String prompt, String placeName) {
        try {
            String url = "https://generativelanguage.googleapis.com/v1beta/models/gemini-pro-vision:generateContent";

            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON);
            headers.set("Authorization", "Bearer " + geminiApiKey);

            // Gemini API 요청 본문 구성
            Map<String, Object> requestBody = new HashMap<>();
            Map<String, Object> contents = new HashMap<>();
            Map<String, Object> parts = new HashMap<>();
            parts.put("text", "Generate an image: " + prompt);
            contents.put("parts", new Object[]{parts});
            requestBody.put("contents", new Object[]{contents});

            // 이미지 생성 설정
            Map<String, Object> generationConfig = new HashMap<>();
            generationConfig.put("temperature", 0.7);
            generationConfig.put("topK", 40);
            generationConfig.put("topP", 0.9);
            requestBody.put("generationConfig", generationConfig);

            HttpEntity<Map<String, Object>> entity = new HttpEntity<>(requestBody, headers);

            ResponseEntity<String> response = restTemplate.postForEntity(url, entity, String.class);

            if (response.getStatusCode().is2xxSuccessful()) {
                JsonNode responseNode = objectMapper.readTree(response.getBody());

                // Gemini 응답에서 이미지 URL 추출
                JsonNode candidates = responseNode.path("candidates");
                if (candidates.isArray() && candidates.size() > 0) {
                    JsonNode content = candidates.get(0).path("content");
                    JsonNode contentParts = content.path("parts");
                    if (contentParts.isArray() && contentParts.size() > 0) {
                        String imageData = contentParts.get(0).path("inlineData").path("data").asText();
                        if (!imageData.isEmpty()) {
                            // Base64 이미지 데이터를 처리하여 URL 반환
                            return processGeminiImageData(imageData, placeName);
                        }
                    }
                }
            }

            logger.error("❌ Gemini API 응답 처리 실패 for {}", placeName);
            return null;

        } catch (Exception e) {
            logger.error("❌ Gemini API 호출 실패 for {}: {}", placeName, e.getMessage());
            return null;
        }
    }

    /**
     * Gemini에서 받은 Base64 이미지 데이터 처리
     */
    private String processGeminiImageData(String base64Data, String placeName) {
        try {
            // Base64 디코딩
            byte[] imageBytes = java.util.Base64.getDecoder().decode(base64Data);

            // 안전한 파일명 생성
            String safeName = placeName.replaceAll("[^a-zA-Z0-9가-힣\\s\\-_]", "")
                                       .replaceAll("\\s+", "_")
                                       .trim();
            if (safeName.length() > 50) {
                safeName = safeName.substring(0, 50);
            }

            String fileName = safeName + "_" + UUID.randomUUID().toString().substring(0, 8) + "_gemini.jpg";

            // 저장 디렉토리 확인 및 생성
            Path storageDir = Paths.get(imageStorageDir);
            if (!Files.exists(storageDir)) {
                Files.createDirectories(storageDir);
                logger.info("Created image storage directory: {}", storageDir);
            }

            // 파일 저장
            Path filePath = storageDir.resolve(fileName);
            Files.write(filePath, imageBytes);

            // 웹 접근 가능한 경로 반환
            String webPath = "/images/places/" + fileName;
            logger.info("✅ Gemini 이미지 저장 완료: {} -> {} ({} bytes)", placeName, webPath, imageBytes.length);

            return webPath;

        } catch (Exception e) {
            logger.error("❌ Gemini 이미지 데이터 처리 실패 for {}: {}", placeName, e.getMessage());
            return null;
        }
    }

    /**
     * Gemini 이미지 다운로드 및 저장
     */
    private String downloadAndSaveGeminiImage(String imageUrl, String placeName) {
        try {
            // 기존 다운로드 메소드 재사용
            return downloadAndSaveImage(imageUrl, placeName);

        } catch (Exception e) {
            logger.error("❌ Gemini 이미지 다운로드 실패 for {}: {}", placeName, e.getMessage());
            return null;
        }
    }
}