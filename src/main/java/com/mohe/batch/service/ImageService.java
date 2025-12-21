package com.mohe.batch.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.*;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Stream;

@Service
public class ImageService {

    private static final Logger log = LoggerFactory.getLogger(ImageService.class);

    private final String storagePath;
    private final HttpClient httpClient;

    public ImageService(@Value("${image.storage-path:/app/images}") String storagePath) {
        this.storagePath = storagePath;
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(30))
                .followRedirects(HttpClient.Redirect.NORMAL)
                .build();

        log.info("ImageService initialized with storage path: {}", storagePath);
    }

    /**
     * 기존 이미지 삭제 후 새 이미지 다운로드
     *
     * @param placeId   장소 ID
     * @param placeName 장소명
     * @param imageUrls 다운로드할 이미지 URL 목록
     * @return 저장된 이미지 경로 목록
     */
    public List<String> downloadAndSaveImages(Long placeId, String placeName, List<String> imageUrls) {
        List<String> savedPaths = new ArrayList<>();

        if (imageUrls == null || imageUrls.isEmpty()) {
            log.debug("No images to download for place ID: {}", placeId);
            return savedPaths;
        }

        try {
            // 1. 장소별 이미지 디렉토리 경로
            Path placeImageDir = Paths.get(storagePath, "places", String.valueOf(placeId));

            // 2. 기존 이미지 디렉토리가 있으면 삭제
            if (Files.exists(placeImageDir)) {
                deleteDirectory(placeImageDir);
                log.info("Deleted existing images for place ID: {}", placeId);
            }

            // 3. 디렉토리 생성
            Files.createDirectories(placeImageDir);

            // 4. 새 이미지 다운로드 및 저장 (최대 5개)
            int maxImages = Math.min(imageUrls.size(), 5);
            for (int i = 0; i < maxImages; i++) {
                String imageUrl = imageUrls.get(i);
                try {
                    String fileName = String.format("%d_%s_%d.jpg",
                            placeId,
                            sanitizeFileName(placeName),
                            i + 1);
                    Path filePath = placeImageDir.resolve(fileName);

                    boolean downloaded = downloadImage(imageUrl, filePath);
                    if (downloaded) {
                        // API에서 접근 가능한 상대 경로 반환
                        String relativePath = "/images/places/" + placeId + "/" + fileName;
                        savedPaths.add(relativePath);
                        log.debug("Saved image: {}", relativePath);
                    }
                } catch (Exception e) {
                    log.warn("Failed to download image {}: {}", imageUrl, e.getMessage());
                }
            }

            log.info("Downloaded {}/{} images for place '{}' (ID: {})",
                    savedPaths.size(), maxImages, placeName, placeId);

        } catch (Exception e) {
            log.error("Error processing images for place ID {}: {}", placeId, e.getMessage());
        }

        return savedPaths;
    }

    /**
     * 디렉토리 삭제 (하위 파일 포함)
     */
    private void deleteDirectory(Path dir) throws IOException {
        if (!Files.exists(dir)) {
            return;
        }

        try (Stream<Path> walk = Files.walk(dir)) {
            walk.sorted(Comparator.reverseOrder())
                    .forEach(path -> {
                        try {
                            Files.delete(path);
                        } catch (IOException e) {
                            log.warn("Failed to delete: {}", path);
                        }
                    });
        }
    }

    /**
     * 이미지 다운로드
     */
    private boolean downloadImage(String imageUrl, Path targetPath) {
        try {
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(imageUrl))
                    .timeout(Duration.ofSeconds(60))
                    .header("User-Agent", "Mozilla/5.0 (compatible; MoheBatch/1.0)")
                    .GET()
                    .build();

            HttpResponse<InputStream> response = httpClient.send(request,
                    HttpResponse.BodyHandlers.ofInputStream());

            if (response.statusCode() == 200) {
                try (InputStream in = response.body()) {
                    Files.copy(in, targetPath, StandardCopyOption.REPLACE_EXISTING);
                    return true;
                }
            } else {
                log.warn("Failed to download image, status: {}, url: {}",
                        response.statusCode(), imageUrl);
                return false;
            }
        } catch (Exception e) {
            log.warn("Error downloading image: {}", e.getMessage());
            return false;
        }
    }

    /**
     * 메뉴 이미지 다운로드
     *
     * @param placeId   장소 ID
     * @param menuIndex 메뉴 인덱스 (1부터 시작)
     * @param imageUrl  이미지 URL
     * @return 저장된 이미지 경로 (실패 시 null)
     */
    public String downloadMenuImage(Long placeId, int menuIndex, String imageUrl) {
        if (imageUrl == null || imageUrl.isEmpty()) {
            return null;
        }

        try {
            // 메뉴 이미지 디렉토리 경로
            Path menuImageDir = Paths.get(storagePath, "places", String.valueOf(placeId), "menus");
            Files.createDirectories(menuImageDir);

            String fileName = String.format("menu_%d.jpg", menuIndex);
            Path filePath = menuImageDir.resolve(fileName);

            boolean downloaded = downloadImage(imageUrl, filePath);
            if (downloaded) {
                String relativePath = "/images/places/" + placeId + "/menus/" + fileName;
                log.debug("Saved menu image: {}", relativePath);
                return relativePath;
            }
        } catch (Exception e) {
            log.warn("Failed to download menu image: {}", e.getMessage());
        }

        return null;
    }

    /**
     * 파일명에 사용할 수 없는 문자 제거
     */
    private String sanitizeFileName(String name) {
        if (name == null || name.isEmpty()) {
            return "unknown";
        }
        return name.replaceAll("[^a-zA-Z0-9가-힣._-]", "_")
                .replaceAll("_{2,}", "_")
                .substring(0, Math.min(name.length(), 50));
    }
}
