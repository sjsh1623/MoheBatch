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
    @Query("""
        SELECT p FROM Place p
        LEFT JOIN p.bookmarks b
        WHERE p.latitude IS NOT NULL AND p.longitude IS NOT NULL
        AND (
            6371 * acos(
                cos(radians(:latitude)) * cos(radians(p.latitude)) *
                cos(radians(p.longitude) - radians(:longitude)) +
                sin(radians(:latitude)) * sin(radians(p.latitude))
            )
        ) <= :distance
        GROUP BY p.id
        ORDER BY COUNT(b) DESC, p.rating DESC
    """)
    List<Place> findMostBookmarkedPlacesWithinDistance(
        @Param("latitude") Double latitude,
        @Param("longitude") Double longitude,
        @Param("distance") Double distance,
        Pageable pageable
    );

    /**
     * Find places with most bookmarks (fallback when no location provided)
     */
    @Query("""
        SELECT p FROM Place p
        LEFT JOIN p.bookmarks b
        GROUP BY p.id
        ORDER BY COUNT(b) DESC, p.rating DESC
    """)
    List<Place> findMostBookmarkedPlaces(Pageable pageable);
}