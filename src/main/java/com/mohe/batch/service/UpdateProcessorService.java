package com.mohe.batch.service;

import com.mohe.batch.dto.crawling.CrawledDataDto;
import com.mohe.batch.dto.crawling.MenuDataDto;
import com.mohe.batch.dto.crawling.MenuItemDto;
import com.mohe.batch.entity.*;
import com.mohe.batch.repository.PlaceRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * 업데이트 처리 서비스
 * - @Transactional로 세션 유지하여 LazyInitializationException 방지
 */
@Service
public class UpdateProcessorService {

    private static final Logger log = LoggerFactory.getLogger(UpdateProcessorService.class);

    private final PlaceRepository placeRepository;
    private final CrawlingService crawlingService;
    private final ImageProcessorClient imageProcessorClient;

    public UpdateProcessorService(
            PlaceRepository placeRepository,
            CrawlingService crawlingService,
            ImageProcessorClient imageProcessorClient
    ) {
        this.placeRepository = placeRepository;
        this.crawlingService = crawlingService;
        this.imageProcessorClient = imageProcessorClient;
    }

    /**
     * Place 업데이트 처리 (트랜잭션 내에서 실행)
     */
    @Transactional
    public Place processUpdate(Long placeId, boolean updateMenus, boolean updateImages, boolean updateReviews) {
        // Fresh한 Place 조회 (세션 내에서)
        Place place = placeRepository.findById(placeId)
                .orElseThrow(() -> new IllegalStateException("Place not found: " + placeId));

        try {
            String searchQuery = place.getRoadAddress();
            if (searchQuery == null || searchQuery.isEmpty()) {
                searchQuery = place.getName();
            }

            log.info("🔄 ========== 업데이트 시작 ========== '{}' (ID: {})", place.getName(), place.getId());

            // 이미지 업데이트
            if (updateImages) {
                updatePlaceImages(place, searchQuery);
            }

            // 메뉴 업데이트
            if (updateMenus) {
                updatePlaceMenus(place, searchQuery);
            }

            // 리뷰 업데이트 (중복 체크)
            if (updateReviews) {
                updatePlaceReviews(place, searchQuery);
            }

            // 처리 완료 표시
            place.setCrawlerFound(true);

            // 저장
            place = placeRepository.saveAndFlush(place);

            log.info("✅ ========== 업데이트 완료 ========== '{}' | 이미지: {} | 메뉴: {} | 리뷰: {}",
                    place.getName(),
                    place.getImages().size(),
                    place.getMenus().size(),
                    place.getReviews().size());

            return place;

        } catch (Exception e) {
            log.error("❌ 업데이트 실패 '{}': {}", place.getName(), e.getMessage());
            place.setCrawlerFound(false);
            return placeRepository.saveAndFlush(place);
        }
    }

    private void updatePlaceImages(Place place, String searchQuery) {
        try {
            log.info("🖼️ 이미지 크롤링 시작 '{}' (ID: {})", place.getName(), place.getId());
            var response = crawlingService.crawlPlaceData(searchQuery, place.getName()).block();

            if (response != null && response.isSuccess() && response.getData() != null) {
                CrawledDataDto data = response.getData();

                // 기존 이미지 파일 삭제
                imageProcessorClient.deletePlaceImages(place.getId());

                // DB 컬렉션 클리어 후 새로 저장
                place.getImages().clear();

                if (data.getImageUrls() != null && !data.getImageUrls().isEmpty()) {
                    List<String> savedPaths = imageProcessorClient.savePlaceImages(
                            place.getId(), place.getName(), data.getImageUrls()
                    );

                    for (int i = 0; i < savedPaths.size(); i++) {
                        PlaceImage placeImage = new PlaceImage();
                        placeImage.setPlace(place);
                        placeImage.setUrl(savedPaths.get(i));
                        placeImage.setOrderIndex(i + 1);
                        place.getImages().add(placeImage);
                    }
                    log.info("🖼️ 장소 이미지 {}개 업데이트 완료", savedPaths.size());
                }
            }
        } catch (Exception e) {
            log.warn("⚠️ 이미지 업데이트 실패 '{}': {}", place.getName(), e.getMessage());
        }
    }

    private void updatePlaceMenus(Place place, String searchQuery) {
        try {
            log.info("🍽️ 메뉴 크롤링 시작 '{}' (ID: {})", place.getName(), place.getId());
            var menuResponse = crawlingService.crawlMenuData(searchQuery, place.getName()).block();

            if (menuResponse != null && menuResponse.isSuccess() && menuResponse.getData() != null) {
                MenuDataDto menuData = menuResponse.getData();

                // 기존 메뉴 이미지 파일 삭제
                imageProcessorClient.deleteMenuImages(place.getId());

                // DB 컬렉션 클리어 후 새로 저장
                place.getMenus().clear();

                if (menuData.getMenus() != null && !menuData.getMenus().isEmpty()) {
                    int menuCount = 0;
                    int maxMenus = Math.min(menuData.getMenus().size(), 50);
                    int menuImageCount = 0;

                    for (int i = 0; i < maxMenus; i++) {
                        MenuItemDto menuItem = menuData.getMenus().get(i);
                        if (menuItem.getName() != null && !menuItem.getName().trim().isEmpty()) {
                            PlaceMenu placeMenu = new PlaceMenu();
                            placeMenu.setPlace(place);
                            placeMenu.setName(sanitizeText(menuItem.getName()));
                            placeMenu.setPrice(sanitizeText(menuItem.getPrice()));
                            placeMenu.setDescription(sanitizeText(menuItem.getDescription()));
                            placeMenu.setImageUrl(menuItem.getImageUrl());
                            placeMenu.setDisplayOrder(i + 1);

                            // 메뉴 이미지 저장
                            if (menuItem.getImageUrl() != null && !menuItem.getImageUrl().isEmpty()) {
                                try {
                                    String menuImagePath = imageProcessorClient.saveMenuImage(
                                            place.getId(), menuItem.getName(), menuItem.getImageUrl()
                                    );
                                    placeMenu.setImagePath(menuImagePath);
                                    if (menuImagePath != null) menuImageCount++;
                                } catch (Exception e) {
                                    log.debug("⚠️ 메뉴 이미지 저장 실패: {}", e.getMessage());
                                }
                            }

                            place.getMenus().add(placeMenu);
                            menuCount++;
                        }
                    }
                    log.info("🍽️ 메뉴 {} / 이미지 {} 업데이트 완료 '{}'",
                            menuCount, menuImageCount, place.getName());
                }
            }
        } catch (Exception e) {
            log.warn("⚠️ 메뉴 업데이트 실패 '{}': {}", place.getName(), e.getMessage());
        }
    }

    private void updatePlaceReviews(Place place, String searchQuery) {
        try {
            log.info("📝 리뷰 크롤링 시작 '{}' (ID: {})", place.getName(), place.getId());
            var response = crawlingService.crawlPlaceData(searchQuery, place.getName()).block();

            if (response != null && response.isSuccess() && response.getData() != null) {
                CrawledDataDto data = response.getData();

                if (data.getReviews() != null && !data.getReviews().isEmpty()) {
                    // 기존 리뷰의 앞 10글자 수집 (중복 체크용)
                    Set<String> existingReviewPrefixes = new HashSet<>();
                    for (PlaceReview review : place.getReviews()) {
                        if (review.getReviewText() != null && review.getReviewText().length() >= 10) {
                            existingReviewPrefixes.add(review.getReviewText().substring(0, 10));
                        } else if (review.getReviewText() != null) {
                            existingReviewPrefixes.add(review.getReviewText());
                        }
                    }

                    int addedCount = 0;
                    int skippedCount = 0;
                    int currentMaxOrder = place.getReviews().stream()
                            .mapToInt(PlaceReview::getOrderIndex)
                            .max()
                            .orElse(0);

                    for (String reviewText : data.getReviews()) {
                        if (reviewText == null || reviewText.trim().isEmpty()) continue;

                        String sanitizedReview = sanitizeText(reviewText);
                        if (sanitizedReview == null || sanitizedReview.trim().isEmpty()) continue;

                        // 중복 체크: 앞 10글자 비교
                        String prefix = sanitizedReview.length() >= 10
                                ? sanitizedReview.substring(0, 10)
                                : sanitizedReview;

                        if (existingReviewPrefixes.contains(prefix)) {
                            skippedCount++;
                            continue;
                        }

                        // 새 리뷰 추가
                        PlaceReview review = new PlaceReview();
                        review.setPlace(place);
                        review.setReviewText(sanitizedReview);
                        review.setOrderIndex(++currentMaxOrder);
                        place.getReviews().add(review);
                        existingReviewPrefixes.add(prefix);
                        addedCount++;

                        // 최대 20개까지만
                        if (place.getReviews().size() >= 20) break;
                    }

                    log.info("📝 리뷰 업데이트: 추가 {} / 중복 스킵 {} / 총 {} '{}'",
                            addedCount, skippedCount, place.getReviews().size(), place.getName());
                }
            }
        } catch (Exception e) {
            log.warn("⚠️ 리뷰 업데이트 실패 '{}': {}", place.getName(), e.getMessage());
        }
    }

    private String sanitizeText(String text) {
        if (text == null) return null;
        return text.replace("\u0000", "")
                .replace("\u0001", "")
                .replace("\u0002", "")
                .replace("\u0003", "")
                .replace("\u0004", "")
                .replace("\u0005", "")
                .replace("\u0006", "")
                .replace("\u0007", "")
                .replace("\u0008", "")
                .replace("\u000B", "")
                .replace("\u000C", "")
                .replace("\u000E", "")
                .replace("\u000F", "");
    }
}
