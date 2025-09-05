package com.mohe.spring.service;

import com.mohe.spring.dto.*;
import com.mohe.spring.entity.Bookmark;
import com.mohe.spring.entity.Place;
import com.mohe.spring.entity.User;
import com.mohe.spring.repository.BookmarkRepository;
import com.mohe.spring.repository.PlaceRepository;
import com.mohe.spring.repository.UserRepository;
import com.mohe.spring.security.UserPrincipal;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

@Service
@Transactional
public class BookmarkService {
    
    private final BookmarkRepository bookmarkRepository;
    private final PlaceRepository placeRepository;
    private final UserRepository userRepository;
    private final SimilarityCalculationService similarityCalculationService;
    
    public BookmarkService(
            BookmarkRepository bookmarkRepository,
            PlaceRepository placeRepository,
            UserRepository userRepository,
            SimilarityCalculationService similarityCalculationService
    ) {
        this.bookmarkRepository = bookmarkRepository;
        this.placeRepository = placeRepository;
        this.userRepository = userRepository;
        this.similarityCalculationService = similarityCalculationService;
    }
    
    public BookmarkToggleResponse toggleBookmark(BookmarkToggleRequest request) {
        User currentUser = getCurrentUser();
        Long placeId = Long.parseLong(request.getPlaceId());
        
        Place place = placeRepository.findById(placeId)
                .orElseThrow(() -> new RuntimeException("장소를 찾을 수 없습니다"));
        
        Optional<Bookmark> existingBookmark = bookmarkRepository.findByUserAndPlace(currentUser, place);
        
        if (existingBookmark.isPresent()) {
            // Remove bookmark
            bookmarkRepository.delete(existingBookmark.get());
            
            // Trigger similarity recalculation for this place
            // This runs async to not impact user experience
            try {
                similarityCalculationService.refreshTopKSimilarities(placeId);
            } catch (Exception ex) {
                // Log error but don't fail the bookmark operation
                // Similarity calculation is not critical for bookmark removal
            }
            
            BookmarkToggleResponse response = new BookmarkToggleResponse();
            response.setBookmarked(false);
            response.setMessage("북마크가 제거되었습니다.");
            
            return response;
        } else {
            // Add bookmark
            Bookmark bookmark = new Bookmark();
            bookmark.setUser(currentUser);
            bookmark.setPlace(place);
            bookmarkRepository.save(bookmark);
            
            // Trigger similarity recalculation for this place
            // This runs async to not impact user experience
            try {
                similarityCalculationService.refreshTopKSimilarities(placeId);
            } catch (Exception ex) {
                // Log error but don't fail the bookmark operation
                // Similarity calculation is not critical for bookmark creation
            }
            
            BookmarkToggleResponse response = new BookmarkToggleResponse();
            response.setBookmarked(true);
            response.setMessage("북마크가 추가되었습니다.");
            
            return response;
        }
    }
    
    public BookmarkListResponse getBookmarks() {
        User currentUser = getCurrentUser();
        Pageable pageable = PageRequest.of(0, 100, Sort.by(Sort.Direction.DESC, "createdAt"));
        Page<Bookmark> bookmarks = bookmarkRepository.findByUserOrderByCreatedAtDesc(currentUser, pageable);
        
        List<BookmarkData> bookmarkData = bookmarks.getContent().stream()
                .map(bookmark -> {
                    BookmarkPlaceData placeData = new BookmarkPlaceData();
                    placeData.setId(bookmark.getPlace().getId().toString());
                    placeData.setName(bookmark.getPlace().getName());
                    placeData.setLocation(bookmark.getPlace().getLocation());
                    placeData.setImage(bookmark.getPlace().getImageUrl());
                    placeData.setRating(bookmark.getPlace().getRating());
                    
                    BookmarkData data = new BookmarkData();
                    data.setId(bookmark.getId().toString());
                    data.setPlace(placeData);
                    data.setCreatedAt(bookmark.getCreatedAt());
                    
                    return data;
                })
                .collect(Collectors.toList());
        
        BookmarkListResponse response = new BookmarkListResponse();
        response.setBookmarks(bookmarkData);
        
        return response;
    }
    
    private User getCurrentUser() {
        UserPrincipal userPrincipal = (UserPrincipal) SecurityContextHolder.getContext().getAuthentication().getPrincipal();
        return userRepository.findById(userPrincipal.getId())
                .orElseThrow(() -> new RuntimeException("사용자를 찾을 수 없습니다"));
    }
}