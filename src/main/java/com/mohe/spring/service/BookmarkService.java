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
        Place place = resolvePlace(request.getPlaceId());

        Optional<Bookmark> existingBookmark = bookmarkRepository.findByUserAndPlace(currentUser, place);

        if (existingBookmark.isPresent()) {
            return removeBookmarkInternal(currentUser, place);
        }

        return addBookmarkInternal(currentUser, place);
    }

    public BookmarkToggleResponse addBookmark(String placeId) {
        User currentUser = getCurrentUser();
        Place place = resolvePlace(placeId);

        if (bookmarkRepository.existsByUserAndPlace(currentUser, place)) {
            return new BookmarkToggleResponse(true, "이미 북마크된 장소입니다.");
        }

        return addBookmarkInternal(currentUser, place);
    }

    public BookmarkToggleResponse removeBookmark(String placeId) {
        User currentUser = getCurrentUser();
        Place place = resolvePlace(placeId);

        Optional<Bookmark> existingBookmark = bookmarkRepository.findByUserAndPlace(currentUser, place);
        if (existingBookmark.isEmpty()) {
            return new BookmarkToggleResponse(false, "북마크되지 않은 장소입니다.");
        }

        return removeBookmarkInternal(currentUser, place);
    }

    public BookmarkStatusResponse getBookmarkStatus(String placeId) {
        User currentUser = getCurrentUser();
        Place place = resolvePlace(placeId);
        boolean exists = bookmarkRepository.existsByUserAndPlace(currentUser, place);
        return new BookmarkStatusResponse(exists);
    }
    
    public BookmarkListResponse getBookmarks(int page, int size) {
        User currentUser = getCurrentUser();
        int safePage = Math.max(page, 0);
        int safeSize = Math.min(Math.max(size, 1), 100);
        Pageable pageable = PageRequest.of(safePage, safeSize, Sort.by(Sort.Direction.DESC, "createdAt"));
        Page<Bookmark> bookmarks = bookmarkRepository.findByUserOrderByCreatedAtDesc(currentUser, pageable);
        
        List<BookmarkData> bookmarkData = bookmarks.getContent().stream()
                .map(bookmark -> {
                    BookmarkPlaceData placeData = new BookmarkPlaceData();
                    placeData.setId(bookmark.getPlace().getId().toString());
                    placeData.setName(bookmark.getPlace().getName());
                    placeData.setLocation(bookmark.getPlace().getAddress());
                    // Get first image from gallery or null
                    String imageUrl = bookmark.getPlace().getGallery() != null && !bookmark.getPlace().getGallery().isEmpty() ? 
                        bookmark.getPlace().getGallery().get(0) : null;
                    placeData.setImage(imageUrl);
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
        response.setPage(bookmarks.getNumber());
        response.setSize(bookmarks.getSize());
        response.setTotalPages(bookmarks.getTotalPages());
        response.setTotalElements(bookmarks.getTotalElements());

        return response;
    }
    
    private User getCurrentUser() {
        UserPrincipal userPrincipal = (UserPrincipal) SecurityContextHolder.getContext().getAuthentication().getPrincipal();
        return userRepository.findById(userPrincipal.getId())
                .orElseThrow(() -> new RuntimeException("사용자를 찾을 수 없습니다"));
    }

    private Place resolvePlace(String placeId) {
        Long id;
        try {
            id = Long.parseLong(placeId);
        } catch (NumberFormatException ex) {
            throw new RuntimeException("유효하지 않은 장소 ID입니다");
        }

        return placeRepository.findById(id)
                .orElseThrow(() -> new RuntimeException("장소를 찾을 수 없습니다"));
    }

    private BookmarkToggleResponse addBookmarkInternal(User user, Place place) {
        Bookmark bookmark = new Bookmark();
        bookmark.setUser(user);
        bookmark.setPlace(place);
        bookmarkRepository.save(bookmark);

        recalculateSimilarities(place.getId());

        return new BookmarkToggleResponse(true, "북마크가 추가되었습니다.");
    }

    private BookmarkToggleResponse removeBookmarkInternal(User user, Place place) {
        bookmarkRepository.deleteByUserAndPlace(user, place);

        recalculateSimilarities(place.getId());

        return new BookmarkToggleResponse(false, "북마크가 제거되었습니다.");
    }

    private void recalculateSimilarities(Long placeId) {
        if (placeId == null) {
            return;
        }

        try {
            similarityCalculationService.refreshTopKSimilarities(placeId);
        } catch (Exception ex) {
            // Ignore recalculation errors; bookmark action should still succeed
        }
    }
}
