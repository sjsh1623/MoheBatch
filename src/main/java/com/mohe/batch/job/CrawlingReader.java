package com.mohe.batch.job;

import com.mohe.batch.entity.Place;
import com.mohe.batch.repository.PlaceRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ItemReader;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;

import java.util.ArrayList;
import java.util.List;

/**
 * 워커별 분산 처리를 위한 CrawlingReader
 *
 * - ID % totalWorkers = workerId 로 워커별 데이터 분산
 * - crawler_found = false 인 데이터만 처리
 * - ORDER BY id ASC 로 순서대로 처리
 */
public class CrawlingReader implements ItemReader<Place> {

    private static final Logger log = LoggerFactory.getLogger(CrawlingReader.class);

    private final PlaceRepository placeRepository;
    private final int workerId;
    private final int totalWorkers;
    private final int pageSize;

    private List<Long> currentPageIds;
    private int currentIndex = 0;
    private int currentPage = 0;
    private boolean hasMorePages = true;
    private boolean initialized = false;

    public CrawlingReader(PlaceRepository placeRepository, int workerId, int totalWorkers, int pageSize) {
        this.placeRepository = placeRepository;
        this.workerId = workerId;
        this.totalWorkers = totalWorkers;
        this.pageSize = pageSize;
    }

    @Override
    public Place read() throws Exception {
        // 초기화
        if (!initialized) {
            long totalCount = placeRepository.countUnprocessedPlaces(workerId, totalWorkers);
            log.info("Worker {} initialized - {} places to process (total workers: {})",
                    workerId, totalCount, totalWorkers);
            initialized = true;
        }

        // 현재 페이지가 비었거나 다 읽었으면 다음 페이지 로드
        if (currentPageIds == null || currentIndex >= currentPageIds.size()) {
            if (!hasMorePages && currentPageIds != null && currentIndex >= currentPageIds.size()) {
                log.info("Worker {} completed - all places processed", workerId);
                return null;
            }

            loadNextPage();

            if (currentPageIds.isEmpty()) {
                log.info("Worker {} completed - no more places to process", workerId);
                return null;
            }
        }

        // 현재 페이지에서 다음 ID 가져오기
        Long placeId = currentPageIds.get(currentIndex);
        currentIndex++;

        // Place 엔티티 로드 (descriptions 포함)
        Place place = placeRepository.findByIdWithCollections(placeId).orElse(null);

        if (place != null) {
            // 다른 컬렉션들도 강제 로드 (LazyInitializationException 방지)
            place.getImages().size();
            place.getBusinessHours().size();
            place.getSns().size();
            place.getReviews().size();
            place.getMenus().size();

            log.debug("Worker {} - Reading place {}/{} in page {}: ID={}, Name='{}'",
                    workerId, currentIndex, currentPageIds.size(), currentPage, place.getId(), place.getName());
        } else {
            log.warn("Worker {} - Place with ID={} not found", workerId, placeId);
        }

        return place;
    }

    private void loadNextPage() {
        if (!hasMorePages) {
            currentPageIds = new ArrayList<>();
            return;
        }

        Pageable pageable = PageRequest.of(currentPage, pageSize, Sort.by("id").ascending());
        Page<Long> idsPage = placeRepository.findUnprocessedPlaceIds(workerId, totalWorkers, pageable);

        currentPageIds = new ArrayList<>(idsPage.getContent());
        currentIndex = 0;
        hasMorePages = idsPage.hasNext();
        currentPage++;

        if (!currentPageIds.isEmpty()) {
            log.info("Worker {} - Loaded page {}: {} IDs (hasNext: {})",
                    workerId, currentPage, currentPageIds.size(), hasMorePages);
        }
    }
}
