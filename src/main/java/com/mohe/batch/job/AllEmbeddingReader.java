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
 * All Embedding Reader
 * - 키워드 또는 메뉴 임베딩이 필요한 장소 읽기
 * - crawl_status = COMPLETED
 * - embed_status = PENDING OR menu_embed_status = PENDING
 * - ORDER BY id ASC
 * - 순차 처리 (병렬 없음)
 */
public class AllEmbeddingReader implements ItemReader<Place> {

    private static final Logger log = LoggerFactory.getLogger(AllEmbeddingReader.class);

    private final PlaceRepository placeRepository;
    private final int pageSize;

    private List<Long> currentPageIds;
    private int currentIndex = 0;
    private int currentPage = 0;
    private boolean hasMorePages = true;
    private boolean initialized = false;

    public AllEmbeddingReader(PlaceRepository placeRepository, int pageSize) {
        this.placeRepository = placeRepository;
        this.pageSize = pageSize;
    }

    @Override
    public Place read() throws Exception {
        // Initialize
        if (!initialized) {
            long totalCount = placeRepository.countPlacesForAllEmbedding();
            log.info("AllEmbeddingReader initialized - {} places to process (crawl_status=COMPLETED, embed_status=PENDING or menu_embed_status=PENDING)",
                    totalCount);
            initialized = true;
        }

        // Load next page if needed
        if (currentPageIds == null || currentIndex >= currentPageIds.size()) {
            if (!hasMorePages && currentPageIds != null && currentIndex >= currentPageIds.size()) {
                log.info("AllEmbeddingReader completed - all places processed");
                return null;
            }

            loadNextPage();

            if (currentPageIds.isEmpty()) {
                log.info("AllEmbeddingReader completed - no more places to process");
                return null;
            }
        }

        // Get next ID
        Long placeId = currentPageIds.get(currentIndex);
        currentIndex++;

        // Load Place entity with keywords and menus
        Place place = placeRepository.findByIdForAllEmbedding(placeId).orElse(null);

        if (place != null) {
            // Force load collections
            if (place.getKeyword() != null) {
                place.getKeyword().size();
            }
            if (place.getMenus() != null) {
                place.getMenus().size();
            }

            log.debug("Reading place {}/{} in page {}: ID={}, Name='{}', Keywords={}, Menus={}",
                    currentIndex, currentPageIds.size(), currentPage,
                    place.getId(), place.getName(),
                    place.getKeyword() != null ? place.getKeyword().size() : 0,
                    place.getMenus() != null ? place.getMenus().size() : 0);
        } else {
            log.warn("Place with ID={} not found", placeId);
        }

        return place;
    }

    private void loadNextPage() {
        if (!hasMorePages) {
            currentPageIds = new ArrayList<>();
            return;
        }

        Pageable pageable = PageRequest.of(currentPage, pageSize, Sort.by("id").ascending());
        Page<Long> idsPage = placeRepository.findPlaceIdsForAllEmbedding(pageable);

        currentPageIds = new ArrayList<>(idsPage.getContent());
        currentIndex = 0;
        hasMorePages = idsPage.hasNext();
        currentPage++;

        if (!currentPageIds.isEmpty()) {
            log.info("Loaded page {}: {} IDs (hasNext: {})", currentPage, currentPageIds.size(), hasMorePages);
        }
    }
}
