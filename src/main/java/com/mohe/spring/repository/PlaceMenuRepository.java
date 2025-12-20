package com.mohe.spring.repository;

import com.mohe.spring.entity.PlaceMenu;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface PlaceMenuRepository extends JpaRepository<PlaceMenu, Long> {

    List<PlaceMenu> findByPlaceIdOrderByDisplayOrderAsc(Long placeId);

    List<PlaceMenu> findByPlaceId(Long placeId);

    @Query("SELECT pm FROM PlaceMenu pm WHERE pm.place.id = :placeId AND pm.name = :name")
    List<PlaceMenu> findByPlaceIdAndName(@Param("placeId") Long placeId, @Param("name") String name);

    @Query("SELECT COUNT(pm) FROM PlaceMenu pm WHERE pm.place.id = :placeId")
    long countByPlaceId(@Param("placeId") Long placeId);

    @Modifying
    @Query("DELETE FROM PlaceMenu pm WHERE pm.place.id = :placeId")
    void deleteByPlaceId(@Param("placeId") Long placeId);

    @Query("SELECT pm FROM PlaceMenu pm WHERE pm.place.id = :placeId AND pm.isPopular = true ORDER BY pm.displayOrder ASC")
    List<PlaceMenu> findPopularMenusByPlaceId(@Param("placeId") Long placeId);

    @Query("SELECT pm FROM PlaceMenu pm WHERE pm.imageUrl IS NOT NULL AND pm.imagePath IS NULL")
    List<PlaceMenu> findMenusWithPendingImageDownload();
}
