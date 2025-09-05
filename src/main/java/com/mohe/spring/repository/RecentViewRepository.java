package com.mohe.spring.repository;

import com.mohe.spring.entity.RecentView;
import com.mohe.spring.entity.User;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.Optional;

@Repository
public interface RecentViewRepository extends JpaRepository<RecentView, Long> {
    
    Page<RecentView> findByUserOrderByViewedAtDesc(User user, Pageable pageable);
    
    List<RecentView> findByUserOrderByViewedAtDesc(User user);
    
    @Query("SELECT rv FROM RecentView rv WHERE rv.user.id = :userId AND rv.place.id = :placeId")
    Optional<RecentView> findByUserIdAndPlaceId(@Param("userId") Long userId, @Param("placeId") Long placeId);
    
    @Modifying
    @Query("UPDATE RecentView rv SET rv.viewedAt = :viewedAt WHERE rv.user.id = :userId AND rv.place.id = :placeId")
    void updateViewedAt(@Param("userId") Long userId, @Param("placeId") Long placeId, @Param("viewedAt") OffsetDateTime viewedAt);
    
    @Modifying
    @Query("DELETE FROM RecentView rv WHERE rv.user.id = :userId AND rv.viewedAt < :cutoffTime")
    void deleteOldViewsByUser(@Param("userId") Long userId, @Param("cutoffTime") OffsetDateTime cutoffTime);
}