package com.mohe.spring.service;

import com.mohe.spring.dto.MyPlacesResponse;
import com.mohe.spring.dto.RecentPlaceData;
import com.mohe.spring.dto.RecentPlacesResponse;
import com.mohe.spring.dto.SimplePlaceDto;
import com.mohe.spring.entity.Bookmark;
import com.mohe.spring.entity.Place;
import com.mohe.spring.entity.RecentView;
import com.mohe.spring.entity.User;
import com.mohe.spring.repository.BookmarkRepository;
import com.mohe.spring.repository.PlaceRepository;
import com.mohe.spring.repository.RecentViewRepository;
import com.mohe.spring.repository.UserRepository;
import com.mohe.spring.security.UserPrincipal;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.stream.Collectors;

@Service
@Transactional
public class ActivityService {
    
    private final RecentViewRepository recentViewRepository;
    private final UserRepository userRepository;
    private final PlaceRepository placeRepository;
    private final BookmarkRepository bookmarkRepository;
    
    public ActivityService(RecentViewRepository recentViewRepository,
                           UserRepository userRepository,
                           PlaceRepository placeRepository,
                           BookmarkRepository bookmarkRepository) {
        this.recentViewRepository = recentViewRepository;
        this.userRepository = userRepository;
        this.placeRepository = placeRepository;
        this.bookmarkRepository = bookmarkRepository;
    }
    
    public RecentPlacesResponse getRecentPlaces() {
        User currentUser = getCurrentUser();
        Pageable pageable = PageRequest.of(0, 50, Sort.by(Sort.Direction.DESC, "viewedAt"));
        Page<RecentView> recentViews = recentViewRepository.findByUserOrderByViewedAtDesc(currentUser, pageable);
        
        List<RecentPlaceData> recentPlaces = recentViews.getContent().stream()
                .map(recentView -> {
                    RecentPlaceData data = new RecentPlaceData();
                    data.setId(recentView.getPlace().getId().toString());
                    data.setTitle(recentView.getPlace().getTitle());
                    data.setLocation(recentView.getPlace().getAddress());
                    // Get first image from gallery or null
                    String imageUrl = recentView.getPlace().getGallery() != null && !recentView.getPlace().getGallery().isEmpty() ? 
                        recentView.getPlace().getGallery().get(0) : null;
                    data.setImage(imageUrl);
                    data.setRating(recentView.getPlace().getRating());
                    data.setViewedAt(recentView.getViewedAt());
                    data.setViewCount(1);
                    return data;
                })
                .collect(Collectors.toList());
        
        RecentPlacesResponse response = new RecentPlacesResponse();
        response.setRecentPlaces(recentPlaces);
        response.setTotalCount((int) recentViews.getTotalElements());
        
        return response;
    }
    
    public MyPlacesResponse getMyPlaces() {
        User currentUser = getCurrentUser();
        List<Bookmark> bookmarks = bookmarkRepository.findByUserOrderByCreatedAtDesc(currentUser);

        List<SimplePlaceDto> places = bookmarks.stream()
            .map(this::convertBookmarkToSimplePlace)
            .collect(Collectors.toList());

        MyPlacesResponse response = new MyPlacesResponse();
        response.setPlaces(places);
        response.setTotalCount(places.size());
        response.setPage(0);
        response.setSize(places.size());
        response.setHasNext(false);

        return response;
    }

    public void recordRecentPlaceView(String placeId) {
        User currentUser = getCurrentUser();
        Long parsedPlaceId;
        try {
            parsedPlaceId = Long.parseLong(placeId);
        } catch (NumberFormatException ex) {
            throw new RuntimeException("유효하지 않은 장소 ID입니다");
        }

        Place place = placeRepository.findById(parsedPlaceId)
                .orElseThrow(() -> new RuntimeException("장소를 찾을 수 없습니다"));

        recentViewRepository.findByUserIdAndPlaceId(currentUser.getId(), place.getId())
            .ifPresentOrElse(recentView -> {
                recentView.setViewedAt(OffsetDateTime.now());
                recentViewRepository.save(recentView);
            }, () -> {
                RecentView recentView = new RecentView(currentUser, place);
                recentView.setViewedAt(OffsetDateTime.now());
                recentViewRepository.save(recentView);
            });

        recentViewRepository.deleteOldViewsByUser(currentUser.getId(), OffsetDateTime.now().minusDays(30));
    }
    
    private User getCurrentUser() {
        UserPrincipal userPrincipal = (UserPrincipal) SecurityContextHolder.getContext().getAuthentication().getPrincipal();
        return userRepository.findById(userPrincipal.getId())
                .orElseThrow(() -> new RuntimeException("사용자를 찾을 수 없습니다"));
    }

    private SimplePlaceDto convertBookmarkToSimplePlace(Bookmark bookmark) {
        Place place = bookmark.getPlace();

        SimplePlaceDto dto = new SimplePlaceDto(
            place.getId().toString(),
            place.getName(),
            place.getCategory(),
            place.getRating() != null ? place.getRating().doubleValue() : null,
            place.getAddress(),
            getPrimaryImage(place)
        );

        dto.setReviewCount(place.getReviewCount());
        dto.setAddress(place.getAddress());
        dto.setIsBookmarked(true);
        dto.setTags(place.getTags());
        dto.setImages(place.getGallery());
        dto.setDescription(place.getDescription());

        return dto;
    }

    private String getPrimaryImage(Place place) {
        if (place.getGallery() != null && !place.getGallery().isEmpty()) {
            return place.getGallery().get(0);
        }
        return null;
    }
}
