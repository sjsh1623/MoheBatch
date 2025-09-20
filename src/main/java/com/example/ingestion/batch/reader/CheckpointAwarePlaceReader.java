package com.example.ingestion.batch.reader;

import com.example.ingestion.entity.BatchCheckpoint;
import com.example.ingestion.service.CheckpointService;
import com.example.ingestion.service.CheckpointService.RegionInfo;
import com.example.ingestion.service.GovernmentApiService;
import com.example.ingestion.service.GovernmentApiService.AdministrativeRegion;
import com.example.ingestion.service.NaverApiService;
import com.example.ingestion.dto.NaverPlaceItem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ItemReader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 체크포인트 기반 장소 읽기 구현체
 * 정부 API에서 행정구역을 가져와 각 지역별로 Naver API를 호출하고
 * 중단된 지점부터 재시작할 수 있도록 체크포인트를 관리
 */
@Component
public class CheckpointAwarePlaceReader implements ItemReader<EnrichedPlace> {

    private static final Logger logger = LoggerFactory.getLogger(CheckpointAwarePlaceReader.class);

    @Autowired
    private CheckpointService checkpointService;

    @Autowired
    private GovernmentApiService governmentApiService;

    @Autowired
    private NaverApiService naverApiService;

    private static final String BATCH_NAME = "place-ingestion-batch";
    private static final String REGION_TYPE = "sigungu"; // 시/군/구 단위로 처리

    private final AtomicBoolean initialized = new AtomicBoolean(false);
    private final AtomicInteger processedCount = new AtomicInteger(0);

    private Iterator<EnrichedPlace> currentBatch;
    private BatchCheckpoint currentCheckpoint;
    private List<AdministrativeRegion> allRegions;

    @Override
    public EnrichedPlace read() throws Exception {
        if (!initialized.getAndSet(true)) {
            initializeReader();
        }

        // 현재 배치에서 다음 아이템 반환
        if (currentBatch != null && currentBatch.hasNext()) {
            EnrichedPlace place = currentBatch.next();
            processedCount.incrementAndGet();
            return place;
        }

        // 현재 배치가 끝났으면 현재 지역 완료 처리 및 다음 지역으로 이동
        if (currentCheckpoint != null) {
            int placesProcessed = processedCount.getAndSet(0);
            checkpointService.markRegionAsCompleted(currentCheckpoint.getId(), placesProcessed);
            logger.info("✅ 지역 완료: {} ({} places processed)",
                       currentCheckpoint.getRegionName(), placesProcessed);
        }

        // 다음 처리할 지역 조회
        Optional<BatchCheckpoint> nextRegion = checkpointService.getNextPendingRegion(BATCH_NAME, REGION_TYPE);
        if (nextRegion.isEmpty()) {
            logger.info("🎉 모든 지역 처리 완료!");
            checkpointService.completeBatchExecution(BATCH_NAME);
            return null; // 배치 종료
        }

        // 다음 지역 처리 시작
        currentCheckpoint = nextRegion.get();
        checkpointService.markRegionAsProcessing(currentCheckpoint.getId());

        logger.info("🔄 새 지역 처리 시작: {} ({})",
                   currentCheckpoint.getRegionName(), currentCheckpoint.getRegionCode());

        // 해당 지역의 장소 데이터 수집
        List<EnrichedPlace> places = fetchPlacesForRegion(currentCheckpoint);
        currentBatch = places.iterator();
        processedCount.set(0);

        // 다음 아이템 반환
        return currentBatch.hasNext() ? currentBatch.next() : read();
    }

    /**
     * 리더 초기화 - 정부 API에서 행정구역 목록을 가져와 체크포인트 설정
     */
    private void initializeReader() {
        try {
            logger.info("🚀 CheckpointAwarePlaceReader 초기화 시작");

            // 배치 실행 시작
            checkpointService.startBatchExecution(BATCH_NAME);

            // 정부 API에서 시/군/구 목록 조회 및 체크포인트 초기화
            logger.info("🏛️ 정부 API에서 행정구역 목록 조회 중...");

            allRegions = new ArrayList<>();
            List<AdministrativeRegion> regions = governmentApiService.getAllSidos()
                .flatMap(sido -> governmentApiService.getSigunguBySido(sido.getRegionCode()))
                .collectList()
                .block(); // 동기적으로 완료 대기

            if (regions != null) {
                allRegions.addAll(regions);
                logger.info("📍 총 {} 개 시/군/구 발견", regions.size());

                // 체크포인트 초기화
                List<RegionInfo> regionInfoList = regions.stream()
                    .map(region -> new RegionInfo(
                        region.getRegionCode(),
                        region.getSigungu() != null ? region.getSigungu() : region.getSido(),
                        region.getRegionCode().substring(0, 2) // 상위 시/도 코드
                    ))
                    .toList();

                checkpointService.initializeRegionCheckpoints(BATCH_NAME, REGION_TYPE, regionInfoList);
            }

            logger.info("✅ CheckpointAwarePlaceReader 초기화 완료 - {} 개 지역", allRegions.size());

        } catch (Exception e) {
            logger.error("❌ CheckpointAwarePlaceReader 초기화 실패", e);
            checkpointService.failBatchExecution(BATCH_NAME);
            throw new RuntimeException("리더 초기화 실패", e);
        }
    }

    /**
     * 특정 지역의 장소 데이터 수집
     */
    private List<EnrichedPlace> fetchPlacesForRegion(BatchCheckpoint checkpoint) {
        List<EnrichedPlace> places = new ArrayList<>();

        try {
            String regionName = checkpoint.getRegionName();
            logger.info("🔍 {} 지역 장소 검색 시작", regionName);

            // 정부 API 설정에서 정의된 검색 키워드 사용
            String[] searchQueries = {
                "카페", "레스토랑", "음식점", "베이커리", "디저트",
                "공원", "박물관", "미술관", "서점", "영화관",
                "헬스장", "요가", "필라테스", "갤러리", "문화센터"
            };

            for (String query : searchQueries) {
                try {
                    String searchTerm = regionName + " " + query;
                    logger.debug("  🔎 검색: {}", searchTerm);

                    List<NaverPlaceItem> naverPlaces = naverApiService.searchPlacesByQuery(searchTerm)
                        .collectList()
                        .block();

                    if (naverPlaces != null) {
                        for (NaverPlaceItem naverPlace : naverPlaces) {
                            // EnrichedPlace 생성 (Google 데이터는 나중에 processor에서 추가)
                            EnrichedPlace enrichedPlace = new EnrichedPlace(naverPlace, null);
                            places.add(enrichedPlace);
                        }
                        logger.debug("    ✅ {} 개 장소 발견", naverPlaces.size());
                    }

                    // API 호출 제한 준수
                    Thread.sleep(200);

                } catch (Exception e) {
                    logger.warn("⚠️ {} 검색 실패: {}", query, e.getMessage());
                }

                // 지역당 최대 100개 장소로 제한 (성능 고려)
                if (places.size() >= 100) {
                    break;
                }
            }

            logger.info("📊 {} 지역에서 총 {} 개 장소 수집", regionName, places.size());

        } catch (Exception e) {
            String errorMessage = String.format("지역 %s 장소 수집 실패: %s",
                                               checkpoint.getRegionName(), e.getMessage());
            logger.error("❌ {}", errorMessage, e);

            // 체크포인트를 실패로 마킹
            checkpointService.markRegionAsFailed(checkpoint.getId(), errorMessage);

            // 빈 리스트 반환하여 다음 지역으로 계속 진행
            return new ArrayList<>();
        }

        return places;
    }

    /**
     * 리더 리셋 (테스트용)
     */
    public void reset() {
        initialized.set(false);
        processedCount.set(0);
        currentBatch = null;
        currentCheckpoint = null;
        allRegions = null;
        logger.info("🔄 CheckpointAwarePlaceReader 리셋 완료");
    }

    /**
     * 현재 진행 상태 조회
     */
    public CheckpointService.BatchProgress getProgress() {
        return checkpointService.getBatchProgress(BATCH_NAME);
    }

    /**
     * 중단된 배치가 있는지 확인
     */
    public boolean hasInterruptedBatch() {
        return checkpointService.hasInterruptedBatch(BATCH_NAME);
    }
}