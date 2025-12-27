package com.mohe.batch.service;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

/**
 * MoheImageProcessor 서버 클라이언트
 * - Node.js 이미지 프로세서를 통해 이미지를 저장
 * - POST /save: URL에서 이미지 다운로드 → 로컬 저장
 */
@Service
public class ImageProcessorClient {

    private static final Logger log = LoggerFactory.getLogger(ImageProcessorClient.class);

    private final WebClient webClient;
    private final String imageProcessorUrl;
    private final String storagePath;

    public ImageProcessorClient(
            @Value("${image.processor-url:http://localhost:3000}") String imageProcessorUrl,
            @Value("${image.storage-path:/app/images}") String storagePath
    ) {
        this.imageProcessorUrl = imageProcessorUrl;
        this.storagePath = storagePath;
        this.webClient = WebClient.builder()
                .baseUrl(imageProcessorUrl)
                .build();

        log.info("🖼️ ImageProcessorClient initialized: {}, storagePath: {}", imageProcessorUrl, storagePath);
    }

    /**
     * 장소 이미지 저장 (ImageProcessor 서버 사용)
     *
     * @param placeId   장소 ID
     * @param placeName 장소명
     * @param imageUrls 이미지 URL 목록
     * @return 저장된 이미지 경로 목록
     */
    public List<String> savePlaceImages(Long placeId, String placeName, List<String> imageUrls) {
        if (imageUrls == null || imageUrls.isEmpty()) {
            log.debug("No images to save for place: {}", placeName);
            return List.of();
        }

        List<String> savedPaths = new ArrayList<>();
        int maxImages = Math.min(imageUrls.size(), 5);
        String sanitizedName = sanitizeFileName(placeName);

        for (int i = 0; i < maxImages; i++) {
            String imageUrl = imageUrls.get(i);
            String extension = extractExtension(imageUrl);

            // 파일명: place/{placeId}_{placeName}_{index}.{ext}
            String fileName = "place/" + placeId + "_" + sanitizedName + "_" + (i + 1) + "." + extension;

            try {
                String savedFileName = saveImageToProcessor(imageUrl, fileName);
                if (savedFileName != null) {
                    String savedPath = "/images/" + savedFileName;
                    savedPaths.add(savedPath);
                    log.debug("🖼️ 장소 이미지 저장 [{}/{}]: {}", i + 1, maxImages, savedPath);
                }
            } catch (Exception e) {
                log.warn("⚠️ 장소 이미지 저장 실패: {}", e.getMessage());
            }
        }

        log.info("🖼️ 장소 이미지 {}/{}개 저장 완료 '{}' (ID: {})",
                savedPaths.size(), maxImages, placeName, placeId);

        return savedPaths;
    }

    /**
     * 메뉴 이미지 저장 (ImageProcessor 서버 사용)
     *
     * @param placeId  장소 ID
     * @param menuName 메뉴명
     * @param imageUrl 이미지 URL
     * @return 저장된 이미지 경로 또는 null
     */
    public String saveMenuImage(Long placeId, String menuName, String imageUrl) {
        if (imageUrl == null || imageUrl.isEmpty()) {
            return null;
        }

        try {
            String extension = extractExtension(imageUrl);
            String sanitizedMenuName = sanitizeFileName(menuName);
            String hash = generateHash(imageUrl);

            // 파일명: menu/{placeId}_{menuName}_{hash}.{ext}
            String fileName = "menu/" + placeId + "_" + sanitizedMenuName + "_" + hash + "." + extension;

            String savedFileName = saveImageToProcessor(imageUrl, fileName);
            if (savedFileName != null) {
                String savedPath = "/images/" + savedFileName;
                log.debug("🍽️ 메뉴 이미지 저장: {}", savedPath);
                return savedPath;
            }
        } catch (Exception e) {
            log.warn("⚠️ 메뉴 이미지 저장 실패: {}", e.getMessage());
        }

        return null;
    }

    /**
     * ImageProcessor API 호출하여 이미지 저장
     */
    private String saveImageToProcessor(String imageUrl, String fileName) {
        try {
            ImageSaveRequest request = new ImageSaveRequest(imageUrl, fileName);

            ImageSaveResponse response = webClient.post()
                    .uri("/save")
                    .contentType(MediaType.APPLICATION_JSON)
                    .bodyValue(request)
                    .retrieve()
                    .bodyToMono(ImageSaveResponse.class)
                    .timeout(Duration.ofSeconds(60))
                    .onErrorResume(e -> {
                        log.error("❌ ImageProcessor API 호출 실패: {}", e.getMessage());
                        return Mono.empty();
                    })
                    .block();

            if (response != null && response.getFileName() != null) {
                return response.getFileName();
            }

            return null;

        } catch (Exception e) {
            log.error("❌ ImageProcessor 저장 실패: {}", e.getMessage());
            return null;
        }
    }

    /**
     * ImageProcessor 헬스 체크
     */
    public boolean checkHealth() {
        try {
            String response = webClient.get()
                    .uri("/")
                    .retrieve()
                    .bodyToMono(String.class)
                    .timeout(Duration.ofSeconds(5))
                    .block();

            boolean healthy = response != null;
            if (healthy) {
                log.debug("💚 ImageProcessor is healthy: {}", imageProcessorUrl);
            }
            return healthy;

        } catch (Exception e) {
            log.error("💔 ImageProcessor is unreachable: {}", imageProcessorUrl);
            return false;
        }
    }

    /**
     * 장소의 메뉴 이미지 삭제 (placeId로 시작하는 모든 메뉴 이미지)
     *
     * @param placeId 장소 ID
     * @return 삭제된 파일 수
     */
    public int deleteMenuImages(Long placeId) {
        Path menuDir = Paths.get(storagePath, "menu");
        if (!Files.exists(menuDir)) {
            log.debug("메뉴 이미지 디렉토리 없음: {}", menuDir);
            return 0;
        }

        String prefix = placeId + "_";
        int deletedCount = 0;

        try (Stream<Path> files = Files.list(menuDir)) {
            List<Path> toDelete = files
                    .filter(path -> path.getFileName().toString().startsWith(prefix))
                    .toList();

            for (Path path : toDelete) {
                try {
                    Files.delete(path);
                    deletedCount++;
                    log.debug("🗑️ 메뉴 이미지 삭제: {}", path.getFileName());
                } catch (IOException e) {
                    log.warn("⚠️ 메뉴 이미지 삭제 실패: {}", path.getFileName());
                }
            }

            if (deletedCount > 0) {
                log.info("🗑️ 메뉴 이미지 {}개 삭제 완료 (placeId: {})", deletedCount, placeId);
            }

        } catch (IOException e) {
            log.error("❌ 메뉴 이미지 디렉토리 읽기 실패: {}", e.getMessage());
        }

        return deletedCount;
    }

    /**
     * 장소 이미지 삭제 (placeId로 시작하는 모든 장소 이미지)
     *
     * @param placeId 장소 ID
     * @return 삭제된 파일 수
     */
    public int deletePlaceImages(Long placeId) {
        Path placeDir = Paths.get(storagePath, "place");
        if (!Files.exists(placeDir)) {
            log.debug("장소 이미지 디렉토리 없음: {}", placeDir);
            return 0;
        }

        String prefix = placeId + "_";
        int deletedCount = 0;

        try (Stream<Path> files = Files.list(placeDir)) {
            List<Path> toDelete = files
                    .filter(path -> path.getFileName().toString().startsWith(prefix))
                    .toList();

            for (Path path : toDelete) {
                try {
                    Files.delete(path);
                    deletedCount++;
                    log.debug("🗑️ 장소 이미지 삭제: {}", path.getFileName());
                } catch (IOException e) {
                    log.warn("⚠️ 장소 이미지 삭제 실패: {}", path.getFileName());
                }
            }

            if (deletedCount > 0) {
                log.info("🗑️ 장소 이미지 {}개 삭제 완료 (placeId: {})", deletedCount, placeId);
            }

        } catch (IOException e) {
            log.error("❌ 장소 이미지 디렉토리 읽기 실패: {}", e.getMessage());
        }

        return deletedCount;
    }

    /**
     * URL에서 확장자 추출
     */
    private String extractExtension(String url) {
        try {
            String urlWithoutQuery = url.split("\\?")[0];
            int lastDotIndex = urlWithoutQuery.lastIndexOf(".");
            if (lastDotIndex != -1 && lastDotIndex < urlWithoutQuery.length() - 1) {
                String extension = urlWithoutQuery.substring(lastDotIndex + 1).toLowerCase();
                if (extension.matches("(jpg|jpeg|png|gif|webp|bmp)")) {
                    return extension;
                }
            }
        } catch (Exception e) {
            log.debug("확장자 추출 실패, 기본값 사용: {}", url);
        }
        return "jpg";
    }

    /**
     * 파일명에서 특수문자 제거
     */
    private String sanitizeFileName(String name) {
        if (name == null || name.isEmpty()) {
            return "unknown";
        }
        String sanitized = name.replaceAll("[^a-zA-Z0-9가-힣._-]", "_")
                .replaceAll("_{2,}", "_");
        return sanitized.substring(0, Math.min(sanitized.length(), 50));
    }

    /**
     * URL에서 짧은 해시 생성 (8자리)
     */
    private String generateHash(String input) {
        try {
            java.security.MessageDigest md = java.security.MessageDigest.getInstance("MD5");
            byte[] digest = md.digest(input.getBytes());
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < 4; i++) {
                sb.append(String.format("%02x", digest[i]));
            }
            return sb.toString();
        } catch (Exception e) {
            return String.valueOf(Math.abs(input.hashCode()));
        }
    }

    /**
     * API 요청 DTO
     */
    private static class ImageSaveRequest {
        private final String url;
        private final String fileName;

        public ImageSaveRequest(String url, String fileName) {
            this.url = url;
            this.fileName = fileName;
        }

        public String getUrl() { return url; }
        public String getFileName() { return fileName; }
    }

    /**
     * API 응답 DTO
     */
    private static class ImageSaveResponse {
        private String message;

        @JsonProperty("fileName")
        private String fileName;

        public String getMessage() { return message; }
        public void setMessage(String message) { this.message = message; }
        public String getFileName() { return fileName; }
        public void setFileName(String fileName) { this.fileName = fileName; }
    }
}
