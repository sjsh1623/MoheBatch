package com.example.ingestion.repository;

import com.example.ingestion.entity.Place;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.Optional;

/**
 * Place Repository - 기존 Spring 구조와 호환
 */
@Repository
public interface PlaceRepository extends JpaRepository<Place, Long> {

    /**
     * 이름과 주소로 중복 장소 검색
     */
    @Query("SELECT p FROM Place p WHERE LOWER(TRIM(p.name)) = LOWER(TRIM(:name)) " +
           "AND LOWER(TRIM(p.address)) = LOWER(TRIM(:address))")
    Optional<Place> findByNameAndAddress(@Param("name") String name, @Param("address") String address);

    /**
     * 네이버 Place ID로 검색
     */
    Optional<Place> findByNaverPlaceId(String naverPlaceId);

    /**
     * 구글 Place ID로 검색
     */
    Optional<Place> findByGooglePlaceId(String googlePlaceId);

    /**
     * 카테고리별 장소 검색
     */
    List<Place> findByCategoryContainingIgnoreCase(String category);

    /**
     * 이름으로 장소 검색 (부분 일치)
     */
    List<Place> findByNameContainingIgnoreCase(String name);

    /**
     * 특정 소스 플래그를 가진 장소들 검색
     */
    List<Place> findBySourceFlags(String sourceFlags);

    /**
     * 새로운 장소들 검색
     */
    List<Place> findByIsNewPlaceTrue();

    /**
     * 평점 재확인이 필요한 장소들 검색
     */
    List<Place> findByShouldRecheckRatingTrue();

    /**
     * 좌표 범위 내 장소 검색
     */
    @Query("SELECT p FROM Place p WHERE " +
           "p.latitude BETWEEN :minLat AND :maxLat AND " +
           "p.longitude BETWEEN :minLng AND :maxLng")
    List<Place> findPlacesInBounds(
            @Param("minLat") Double minLatitude,
            @Param("maxLat") Double maxLatitude,
            @Param("minLng") Double minLongitude,
            @Param("maxLng") Double maxLongitude
    );

    /**
     * 평점 범위로 장소 검색
     */
    @Query("SELECT p FROM Place p WHERE p.rating BETWEEN :minRating AND :maxRating " +
           "ORDER BY p.rating DESC")
    List<Place> findByRatingRange(
            @Param("minRating") Double minRating,
            @Param("maxRating") Double maxRating
    );

    /**
     * 최근 추가된 장소들 검색 (페이징)
     */
    @Query("SELECT p FROM Place p ORDER BY p.createdAt DESC")
    Page<Place> findRecentPlaces(Pageable pageable);

    /**
     * 인기순 장소 검색 (페이징)
     */
    @Query("SELECT p FROM Place p ORDER BY p.popularity DESC, p.rating DESC")
    Page<Place> findPopularPlaces(Pageable pageable);

    /**
     * 모든 장소 수 카운트
     */
    @Query("SELECT COUNT(p) FROM Place p")
    long countAllPlaces();

    /**
     * 카테고리별 장소 수 카운트
     */
    @Query("SELECT COUNT(p) FROM Place p WHERE p.category = :category")
    long countByCategory(@Param("category") String category);

    /**
     * 소스별 장소 수 카운트
     */
    @Query("SELECT COUNT(p) FROM Place p WHERE p.sourceFlags = :sourceFlags")
    long countBySourceFlags(@Param("sourceFlags") String sourceFlags);

    /**
     * 특정 평점 이상의 장소 수 카운트
     */
    @Query("SELECT COUNT(p) FROM Place p WHERE p.rating >= :minRating")
    long countByMinRating(@Param("minRating") Double minRating);

    /**
     * 배치 처리를 위한 모든 장소 삭제
     */
    @Modifying
    @Transactional
    @Query("DELETE FROM Place p")
    void deleteAllPlaces();

    /**
     * 특정 소스의 장소들만 삭제
     */
    @Modifying
    @Transactional
    @Query("DELETE FROM Place p WHERE p.sourceFlags = :sourceFlags")
    void deleteBySourceFlags(@Param("sourceFlags") String sourceFlags);

    /**
     * 배치 ID별 통계 조회
     */
    @Query("SELECT " +
           "COUNT(p) as totalCount, " +
           "AVG(p.rating) as avgRating, " +
           "COUNT(DISTINCT p.category) as uniqueCategories " +
           "FROM Place p WHERE p.sourceFlags = :sourceFlags")
    Object[] getStatsBySourceFlags(@Param("sourceFlags") String sourceFlags);

    /**
     * 키워드로 장소 검색 (전문 검색)
     */
    @Query(value = "SELECT * FROM places p WHERE " +
                   "to_tsvector('korean', p.name || ' ' || COALESCE(p.description, '')) " +
                   "@@ plainto_tsquery('korean', :keyword) " +
                   "ORDER BY ts_rank(to_tsvector('korean', p.name || ' ' || COALESCE(p.description, '')), " +
                   "plainto_tsquery('korean', :keyword)) DESC",
           nativeQuery = true)
    List<Place> searchByKeyword(@Param("keyword") String keyword);

    /**
     * 중복 장소 검색 (이름 + 주소 유사도 기반)
     */
    @Query(value = "SELECT * FROM places p1 WHERE EXISTS (" +
                   "SELECT 1 FROM places p2 WHERE p1.id != p2.id AND " +
                   "similarity(LOWER(p1.name), LOWER(p2.name)) > 0.8 AND " +
                   "similarity(LOWER(p1.address), LOWER(p2.address)) > 0.7" +
                   ")",
           nativeQuery = true)
    List<Place> findDuplicatePlaces();

    /**
     * 배치 처리용 페이징 조회
     */
    @Query("SELECT p FROM Place p WHERE p.id > :lastId ORDER BY p.id ASC")
    List<Place> findPlacesForBatchProcessing(@Param("lastId") Long lastId, Pageable pageable);

    /**
     * 이미지가 없는 장소들 검색
     */
    @Query("SELECT p FROM Place p WHERE p.imageUrl IS NULL OR p.imageUrl = ''")
    List<Place> findPlacesWithoutImages();

    /**
     * 설명이 없는 장소들 검색
     */
    @Query("SELECT p FROM Place p WHERE p.description IS NULL OR p.description = ''")
    List<Place> findPlacesWithoutDescription();

    /**
     * 최근 업데이트되지 않은 장소들 검색
     */
    @Query("SELECT p FROM Place p WHERE p.updatedAt < :cutoffDate ORDER BY p.updatedAt ASC")
    List<Place> findStalePlaces(@Param("cutoffDate") java.time.ZonedDateTime cutoffDate);
}