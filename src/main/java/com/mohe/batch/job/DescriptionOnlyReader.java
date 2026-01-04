package com.mohe.batch.job;

import com.mohe.batch.entity.Place;
import com.mohe.batch.repository.PlaceRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ItemReader;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;

import java.util.Iterator;
import java.util.List;

/**
 * Description 전용 배치 Reader
 * - 크롤링 완료된 장소 중 mohe_description이 없는 경우
 * - 리뷰가 있는 장소만 대상
 */
public class DescriptionOnlyReader implements ItemReader<Place> {

    private static final Logger log = LoggerFactory.getLogger(DescriptionOnlyReader.class);

    private final PlaceRepository placeRepository;
    private final int pageSize;

    private Iterator<Long> currentPageIds;
    private int currentPage = 0;
    private boolean exhausted = false;

    public DescriptionOnlyReader(PlaceRepository placeRepository, int pageSize) {
        this.placeRepository = placeRepository;
        this.pageSize = pageSize;
        log.info("DescriptionOnlyReader initialized with pageSize={}", pageSize);
    }

    @Override
    public synchronized Place read() {
        // 이미 모든 데이터 처리 완료
        if (exhausted) {
            return null;
        }

        // 현재 페이지 데이터가 없으면 새 페이지 로드
        if (currentPageIds == null || !currentPageIds.hasNext()) {
            loadNextPage();
        }

        // 더 이상 데이터가 없으면 종료
        if (currentPageIds == null || !currentPageIds.hasNext()) {
            log.info("📭 처리할 데이터가 없습니다.");
            exhausted = true;
            return null;
        }

        // ID로 Place 조회 (reviews 포함)
        Long placeId = currentPageIds.next();
        return placeRepository.findByIdWithReviews(placeId).orElse(null);
    }

    private void loadNextPage() {
        log.info("📖 페이지 {} 로딩 중...", currentPage);

        Page<Long> page = placeRepository.findPlaceIdsForDescriptionGeneration(
                PageRequest.of(currentPage, pageSize)
        );

        List<Long> ids = page.getContent();
        if (ids.isEmpty()) {
            log.info("📭 더 이상 처리할 데이터가 없습니다. (총 {} 페이지 처리)", currentPage);
            exhausted = true;
            currentPageIds = null;
            return;
        }

        log.info("📦 페이지 {} 로드 완료: {} 건", currentPage, ids.size());
        currentPageIds = ids.iterator();
        currentPage++;
    }
}
