package com.example.ingestion.repository;

import com.example.ingestion.entity.PlaceImage;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.Optional;

/**
 * PlaceImage Repository - 기존 Spring 구조와 호환
 */
@Repository
public interface PlaceImageRepository extends JpaRepository<PlaceImage, Long> {

    /**
     * 특정 장소의 모든 이미지 조회
     */
    List<PlaceImage> findByPlaceIdOrderByDisplayOrderAsc(Long placeId);

    /**
     * 특정 장소의 Primary 이미지 조회
     */
    Optional<PlaceImage> findByPlaceIdAndIsPrimaryTrue(Long placeId);

    /**
     * 특정 장소의 특정 타입 이미지들 조회
     */
    List<PlaceImage> findByPlaceIdAndImageType(Long placeId, PlaceImage.ImageType imageType);

    /**
     * 특정 소스의 이미지들 조회
     */
    List<PlaceImage> findBySource(PlaceImage.ImageSource source);

    /**
     * 검증된 이미지들 조회
     */
    List<PlaceImage> findByIsVerifiedTrue();

    /**
     * 검증되지 않은 이미지들 조회
     */
    List<PlaceImage> findByIsVerifiedFalse();

    /**
     * 특정 장소의 이미지 수 카운트
     */
    @Query("SELECT COUNT(pi) FROM PlaceImage pi WHERE pi.place.id = :placeId")
    long countByPlaceId(@Param("placeId") Long placeId);

    /**
     * 특정 소스의 이미지 수 카운트
     */
    long countBySource(PlaceImage.ImageSource source);

    /**
     * 특정 타입의 이미지 수 카운트
     */
    long countByImageType(PlaceImage.ImageType imageType);

    /**
     * 중복 이미지 URL 검색
     */
    @Query("SELECT pi FROM PlaceImage pi WHERE pi.imageUrl = :imageUrl AND pi.place.id = :placeId")
    Optional<PlaceImage> findByImageUrlAndPlaceId(@Param("imageUrl") String imageUrl, @Param("placeId") Long placeId);

    /**
     * 특정 장소의 모든 이미지 삭제
     */
    @Modifying
    @Transactional
    void deleteByPlaceId(Long placeId);

    /**
     * 특정 소스의 모든 이미지 삭제
     */
    @Modifying
    @Transactional
    void deleteBySource(PlaceImage.ImageSource source);

    /**
     * 모든 이미지 삭제 (배치 처리용)
     */
    @Modifying
    @Transactional
    @Query("DELETE FROM PlaceImage pi")
    void deleteAllImages();

    /**
     * 특정 장소의 Primary 이미지 설정 해제
     */
    @Modifying
    @Transactional
    @Query("UPDATE PlaceImage pi SET pi.isPrimary = false WHERE pi.place.id = :placeId")
    void unsetPrimaryImagesByPlaceId(@Param("placeId") Long placeId);

    /**
     * 파일 크기별 이미지 조회
     */
    @Query("SELECT pi FROM PlaceImage pi WHERE pi.fileSize BETWEEN :minSize AND :maxSize")
    List<PlaceImage> findByFileSizeRange(@Param("minSize") Long minSize, @Param("maxSize") Long maxSize);

    /**
     * 해상도별 이미지 조회
     */
    @Query("SELECT pi FROM PlaceImage pi WHERE pi.width >= :minWidth AND pi.height >= :minHeight")
    List<PlaceImage> findByMinResolution(@Param("minWidth") Integer minWidth, @Param("minHeight") Integer minHeight);

    /**
     * 소스 ID로 이미지 조회
     */
    List<PlaceImage> findBySourceId(String sourceId);

    /**
     * 특정 장소의 이미지들을 display_order로 정렬하여 조회
     */
    @Query("SELECT pi FROM PlaceImage pi WHERE pi.place.id = :placeId ORDER BY pi.isPrimary DESC, pi.displayOrder ASC")
    List<PlaceImage> findByPlaceIdOrderedForDisplay(@Param("placeId") Long placeId);

    /**
     * 이미지 통계 조회
     */
    @Query("SELECT " +
           "COUNT(pi) as totalImages, " +
           "COUNT(DISTINCT pi.place.id) as placesWithImages, " +
           "AVG(pi.fileSize) as avgFileSize " +
           "FROM PlaceImage pi")
    Object[] getImageStatistics();

    /**
     * 소스별 이미지 통계
     */
    @Query("SELECT pi.source, COUNT(pi) FROM PlaceImage pi GROUP BY pi.source")
    List<Object[]> getImageCountBySource();

    /**
     * 타입별 이미지 통계
     */
    @Query("SELECT pi.imageType, COUNT(pi) FROM PlaceImage pi GROUP BY pi.imageType")
    List<Object[]> getImageCountByType();

    /**
     * 장소별 이미지 수 통계
     */
    @Query("SELECT p.id, p.name, COUNT(pi) as imageCount " +
           "FROM PlaceImage pi RIGHT JOIN pi.place p " +
           "GROUP BY p.id, p.name " +
           "ORDER BY imageCount DESC")
    List<Object[]> getImageCountByPlace();

    /**
     * 이미지가 없는 장소들의 ID 목록
     */
    @Query("SELECT p.id FROM Place p WHERE p.id NOT IN (SELECT DISTINCT pi.place.id FROM PlaceImage pi)")
    List<Long> findPlaceIdsWithoutImages();

    /**
     * Primary 이미지가 없는 장소들의 ID 목록
     */
    @Query("SELECT p.id FROM Place p WHERE p.id NOT IN " +
           "(SELECT DISTINCT pi.place.id FROM PlaceImage pi WHERE pi.isPrimary = true)")
    List<Long> findPlaceIdsWithoutPrimaryImage();

    /**
     * 최근 추가된 이미지들 조회
     */
    @Query("SELECT pi FROM PlaceImage pi ORDER BY pi.createdAt DESC")
    List<PlaceImage> findRecentImages(@Param("limit") int limit);

    /**
     * 특정 기간 이후에 추가된 이미지들 조회
     */
    @Query("SELECT pi FROM PlaceImage pi WHERE pi.createdAt >= :since ORDER BY pi.createdAt DESC")
    List<PlaceImage> findImagesSince(@Param("since") java.time.ZonedDateTime since);
}