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
}