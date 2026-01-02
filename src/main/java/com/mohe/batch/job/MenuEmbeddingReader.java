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
 * Menu Embedding Reader
 * - crawl_status = COMPLETED, menu_embed_status = PENDING 조건
 * - 메뉴가 있는 장소만
 * - ORDER BY id ASC
 * - 순차 처리 (병렬 없음)
 */
public class MenuEmbeddingReader implements ItemReader<Place> {

    private static final Logger log = LoggerFactory.getLogger(MenuEmbeddingReader.class);

    private final PlaceRepository placeRepository;
    private final int pageSize;

    private List<Long> currentPageIds;
    private int currentIndex = 0;
    private int currentPage = 0;
    private boolean hasMorePages = true;
    private boolean initialized = false;

    public MenuEmbeddingReader(PlaceRepository placeRepository, int pageSize) {
        this.placeRepository = placeRepository;
        this.pageSize = pageSize;
    }

    @Override
    public Place read() throws Exception {
        // Initialize
        if (!initialized) {
            long totalCount = placeRepository.countPlacesForMenuEmbedding();
            log.info("MenuEmbeddingReader initialized - {} places to process (crawl_status=COMPLETED, menu_embed_status=PENDING, has menus)",
                    totalCount);
            initialized = true;
        }

        // Load next page if needed
        if (currentPageIds == null || currentIndex >= currentPageIds.size()) {
            if (!hasMorePages && currentPageIds != null && currentIndex >= currentPageIds.size()) {
                log.info("MenuEmbeddingReader completed - all places processed");
                return null;
            }

            loadNextPage();

            if (currentPageIds.isEmpty()) {
                log.info("MenuEmbeddingReader completed - no more places to process");
                return null;
            }
        }

        // Get next ID
        Long placeId = currentPageIds.get(currentIndex);
        currentIndex++;

        // Load Place entity with menus
        Place place = placeRepository.findByIdForMenuEmbedding(placeId).orElse(null);

        if (place != null) {
            // Force load menus collection
            if (place.getMenus() != null) {
                place.getMenus().size();
            }

            log.debug("Reading place {}/{} in page {}: ID={}, Name='{}', Menus={}",
                    currentIndex, currentPageIds.size(), currentPage,
                    place.getId(), place.getName(),
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
        Page<Long> idsPage = placeRepository.findPlaceIdsForMenuEmbedding(pageable);

        currentPageIds = new ArrayList<>(idsPage.getContent());
        currentIndex = 0;
        hasMorePages = idsPage.hasNext();
        currentPage++;

        if (!currentPageIds.isEmpty()) {
            log.info("Loaded page {}: {} IDs (hasNext: {})", currentPage, currentPageIds.size(), hasMorePages);
        }
    }
}
