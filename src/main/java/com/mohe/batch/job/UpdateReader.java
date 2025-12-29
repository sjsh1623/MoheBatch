package com.mohe.batch.job;

import com.mohe.batch.entity.Place;
import com.mohe.batch.repository.PlaceRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ItemReader;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;

import java.util.Iterator;

/**
 * 업데이트용 ItemReader
 * - crawler_found=false인 장소 읽기 (크롤링 대상과 동일)
 * - Worker ID로 분산 처리
 * - OpenAI 없이 메뉴/이미지/리뷰만 업데이트
 */
public class UpdateReader implements ItemReader<Place> {

    private static final Logger log = LoggerFactory.getLogger(UpdateReader.class);

    private final PlaceRepository placeRepository;
    private final int workerId;
    private final int totalWorkers;
    private final int pageSize;

    private int currentPage = 0;
    private Iterator<Place> currentBatch;
    private boolean hasMoreData = true;

    public UpdateReader(PlaceRepository placeRepository, int workerId, int totalWorkers, int pageSize) {
        this.placeRepository = placeRepository;
        this.workerId = workerId;
        this.totalWorkers = totalWorkers;
        this.pageSize = pageSize;

        log.info("🔄 UpdateReader 초기화: workerId={}, totalWorkers={}, pageSize={}, 조건=crawler_found=false",
                workerId, totalWorkers, pageSize);
    }

    @Override
    public synchronized Place read() {
        // 현재 배치에서 읽을 데이터가 있으면 반환
        if (currentBatch != null && currentBatch.hasNext()) {
            return currentBatch.next();
        }

        // 더 이상 데이터가 없으면 null 반환
        if (!hasMoreData) {
            return null;
        }

        // 다음 페이지 로드
        loadNextBatch();

        // 로드 후 데이터가 있으면 반환
        if (currentBatch != null && currentBatch.hasNext()) {
            return currentBatch.next();
        }

        return null;
    }

    private void loadNextBatch() {
        try {
            PageRequest pageRequest = PageRequest.of(currentPage, pageSize, Sort.by("id").ascending());

            // crawl_status=PENDING인 장소 조회 (크롤링 대상)
            Page<Place> page = placeRepository.findByCrawlStatusPendingAndIdModEquals(
                    workerId, totalWorkers, pageRequest
            );

            if (page.hasContent()) {
                currentBatch = page.getContent().iterator();
                log.info("🔄 [Worker {}] 업데이트 대상 로드: 페이지 {}, {}개 장소 (crawl_status=PENDING)",
                        workerId, currentPage, page.getNumberOfElements());
                currentPage++;
            } else {
                hasMoreData = false;
                log.info("🔄 [Worker {}] 모든 업데이트 대상 처리 완료", workerId);
            }

            if (!page.hasNext()) {
                hasMoreData = false;
            }

        } catch (Exception e) {
            log.error("🔄 [Worker {}] 데이터 로드 실패: {}", workerId, e.getMessage());
            hasMoreData = false;
        }
    }
}
