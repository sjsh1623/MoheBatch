package com.mohe.spring.repository;

import com.mohe.spring.entity.Bookmark;
import com.mohe.spring.entity.Place;
import com.mohe.spring.entity.User;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

@Repository
public interface BookmarkRepository extends JpaRepository<Bookmark, Long> {
    
    Optional<Bookmark> findByUserAndPlace(User user, Place place);
    
    boolean existsByUserAndPlace(User user, Place place);
    
    Page<Bookmark> findByUserOrderByCreatedAtDesc(User user, Pageable pageable);
    
    List<Bookmark> findByUserOrderByCreatedAtDesc(User user);
    
    @Query("SELECT b.place.id FROM Bookmark b WHERE b.user.id = :userId")
    List<Long> findBookmarkedPlaceIdsByUserId(@Param("userId") Long userId);
    
    long countByUser(User user);

    void deleteByUserAndPlace(User user, Place place);

    /**
     * Find places with most bookmarks within distance from user location
     */
    @Query(value = """
        SELECT p.id, p.name, p.category, p.rating, p.review_count, p.address, p.description,
               p.latitude, p.longitude, p.phone, p.website_url, p.created_at, p.updated_at,
               p.naver_place_id, p.google_place_id, p.last_rating_check, p.should_recheck_rating,
               p.opened_date, p.first_seen_at, p.is_new_place,
               COUNT(b.id) as bookmark_count
        FROM places p
        LEFT JOIN bookmarks b ON p.id = b.place_id
        WHERE p.latitude IS NOT NULL AND p.longitude IS NOT NULL
        AND (
            6371 * acos(
                cos(radians(?1)) * cos(radians(CAST(p.latitude AS DOUBLE PRECISION))) *
                cos(radians(CAST(p.longitude AS DOUBLE PRECISION)) - radians(?2)) +
                sin(radians(?1)) * sin(radians(CAST(p.latitude AS DOUBLE PRECISION)))
            )
        ) <= ?3
        GROUP BY p.id, p.name, p.category, p.rating, p.review_count, p.address, p.description,
                 p.latitude, p.longitude, p.phone, p.website_url, p.created_at, p.updated_at,
                 p.naver_place_id, p.google_place_id, p.last_rating_check, p.should_recheck_rating,
                 p.opened_date, p.first_seen_at, p.is_new_place
        ORDER BY COUNT(b.id) DESC, p.rating DESC
        LIMIT ?4
    """, nativeQuery = true)
    List<Place> findMostBookmarkedPlacesWithinDistance(
        Double latitude,
        Double longitude,
        Double distance,
        int limit
    );

    /**
     * Find places with most bookmarks (fallback when no location provided)
     */
    @Query(value = """
        SELECT p.id, p.name, p.category, p.rating, p.review_count, p.address, p.description,
               p.latitude, p.longitude, p.phone, p.website_url, p.created_at, p.updated_at,
               p.naver_place_id, p.google_place_id, p.last_rating_check, p.should_recheck_rating,
               p.opened_date, p.first_seen_at, p.is_new_place,
               COUNT(b.id) as bookmark_count
        FROM places p
        LEFT JOIN bookmarks b ON p.id = b.place_id
        GROUP BY p.id, p.name, p.category, p.rating, p.review_count, p.address, p.description,
                 p.latitude, p.longitude, p.phone, p.website_url, p.created_at, p.updated_at,
                 p.naver_place_id, p.google_place_id, p.last_rating_check, p.should_recheck_rating,
                 p.opened_date, p.first_seen_at, p.is_new_place
        ORDER BY COUNT(b.id) DESC, p.rating DESC
        LIMIT ?1
    """, nativeQuery = true)
    List<Place> findMostBookmarkedPlaces(int limit);
}