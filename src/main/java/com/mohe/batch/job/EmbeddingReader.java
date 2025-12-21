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
 * Embedding Reader
 * - crawler_found = true, ready = false 조건
 * - ORDER BY id ASC
 * - 순차 처리 (병렬 없음)
 */
public class EmbeddingReader implements ItemReader<Place> {

    private static final Logger log = LoggerFactory.getLogger(EmbeddingReader.class);

    private final PlaceRepository placeRepository;
    private final int pageSize;

    private List<Long> currentPageIds;
    private int currentIndex = 0;
    private int currentPage = 0;
    private boolean hasMorePages = true;
    private boolean initialized = false;

    public EmbeddingReader(PlaceRepository placeRepository, int pageSize) {
        this.placeRepository = placeRepository;
        this.pageSize = pageSize;
    }

    @Override
    public Place read() throws Exception {
        // Initialize
        if (!initialized) {
            long totalCount = placeRepository.countPlacesForEmbedding();
            log.info("EmbeddingReader initialized - {} places to process (crawler_found=true, ready=false)",
                    totalCount);
            initialized = true;
        }

        // Load next page if needed
        if (currentPageIds == null || currentIndex >= currentPageIds.size()) {
            if (!hasMorePages && currentPageIds != null && currentIndex >= currentPageIds.size()) {
                log.info("EmbeddingReader completed - all places processed");
                return null;
            }

            loadNextPage();

            if (currentPageIds.isEmpty()) {
                log.info("EmbeddingReader completed - no more places to process");
                return null;
            }
        }

        // Get next ID
        Long placeId = currentPageIds.get(currentIndex);
        currentIndex++;

        // Load Place entity with keywords
        Place place = placeRepository.findByIdForEmbedding(placeId).orElse(null);

        if (place != null) {
            // Force load keyword collection
            if (place.getKeyword() != null) {
                place.getKeyword().size();
            }

            log.debug("Reading place {}/{} in page {}: ID={}, Name='{}', Keywords={}",
                    currentIndex, currentPageIds.size(), currentPage,
                    place.getId(), place.getName(),
                    place.getKeyword() != null ? place.getKeyword().size() : 0);
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
        Page<Long> idsPage = placeRepository.findPlaceIdsForEmbedding(pageable);

        currentPageIds = new ArrayList<>(idsPage.getContent());
        currentIndex = 0;
        hasMorePages = idsPage.hasNext();
        currentPage++;

        if (!currentPageIds.isEmpty()) {
            log.info("Loaded page {}: {} IDs (hasNext: {})", currentPage, currentPageIds.size(), hasMorePages);
        }
    }
}
