package com.mohe.batch.repository;

import com.mohe.batch.entity.Place;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.EntityGraph;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.Optional;

@Repository
public interface PlaceRepository extends JpaRepository<Place, Long> {

    /**
     * 워커별 처리 대상 Place ID 조회
     * - crawler_found = false (아직 크롤링 안 됨)
     * - ID % totalWorkers = workerId (워커별 분산)
     * - ORDER BY id ASC (순서대로 처리)
     *
     * @param workerId 현재 워커 ID (0, 1, 2)
     * @param totalWorkers 전체 워커 수 (3)
     * @param pageable 페이지네이션
     * @return 처리할 Place ID 목록
     */
    @Query(value = """
        SELECT p.id FROM places p
        WHERE p.crawler_found = false
        AND MOD(p.id, :totalWorkers) = :workerId
        ORDER BY p.id ASC
    """, nativeQuery = true)
    Page<Long> findUnprocessedPlaceIds(
        @Param("workerId") int workerId,
        @Param("totalWorkers") int totalWorkers,
        Pageable pageable
    );

    /**
     * 워커별 처리 대기 중인 Place 수 조회
     */
    @Query(value = """
        SELECT COUNT(*) FROM places p
        WHERE p.crawler_found = false
        AND MOD(p.id, :totalWorkers) = :workerId
    """, nativeQuery = true)
    long countUnprocessedPlaces(
        @Param("workerId") int workerId,
        @Param("totalWorkers") int totalWorkers
    );

    /**
     * 전체 처리 대기 중인 Place 수 조회
     */
    @Query("SELECT COUNT(p) FROM Place p WHERE p.crawlerFound = false")
    long countAllUnprocessed();

    /**
     * 전체 처리 완료된 Place 수 조회
     */
    @Query("SELECT COUNT(p) FROM Place p WHERE p.crawlerFound = true")
    long countAllProcessed();

    /**
     * Place와 descriptions 컬렉션 함께 조회
     * MultipleBagFetchException 방지를 위해 descriptions만 EntityGraph로 로드
     */
    @EntityGraph(attributePaths = {"descriptions"})
    @Query("SELECT p FROM Place p WHERE p.id = :id")
    Optional<Place> findByIdWithCollections(@Param("id") Long id);

    // ===== Embedding 관련 쿼리 =====

    /**
     * 임베딩 대상 Place ID 조회
     * - crawler_found = true (크롤링 완료)
     * - ready = false (아직 임베딩 안 됨)
     * - ORDER BY id ASC (순서대로 처리)
     */
    @Query(value = """
        SELECT p.id FROM places p
        WHERE p.crawler_found = true
        AND p.ready = false
        ORDER BY p.id ASC
    """, nativeQuery = true)
    Page<Long> findPlaceIdsForEmbedding(Pageable pageable);

    /**
     * 임베딩 대기 중인 Place 수 조회
     */
    @Query("SELECT COUNT(p) FROM Place p WHERE p.crawlerFound = true AND p.ready = false")
    long countPlacesForEmbedding();

    /**
     * 임베딩용 Place 조회 (키워드 포함)
     */
    @Query("SELECT p FROM Place p LEFT JOIN FETCH p.keyword WHERE p.id = :id")
    Optional<Place> findByIdForEmbedding(@Param("id") Long id);

    /**
     * 임베딩 완료된 Place 수 조회
     */
    @Query("SELECT COUNT(p) FROM Place p WHERE p.ready = true")
    long countEmbeddedPlaces();

    // ===== Update 관련 쿼리 =====

    /**
     * 업데이트 대상 Place 조회 (crawler_found = false)
     * - 워커별 분산 처리
     * - 크롤링 대상과 동일 조건 (OpenAI 없이 메뉴/이미지/리뷰만 업데이트)
     */
    @Query(value = """
        SELECT * FROM places p
        WHERE p.crawler_found = false
        AND MOD(p.id, :totalWorkers) = :workerId
        ORDER BY p.id ASC
    """, nativeQuery = true)
    Page<Place> findByCrawlerFoundFalseAndIdModEquals(
        @Param("workerId") int workerId,
        @Param("totalWorkers") int totalWorkers,
        Pageable pageable
    );
}
