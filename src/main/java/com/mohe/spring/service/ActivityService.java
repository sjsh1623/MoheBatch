package com.mohe.spring.service;

import com.mohe.spring.dto.MyPlacesResponse;
import com.mohe.spring.dto.RecentPlaceData;
import com.mohe.spring.dto.RecentPlacesResponse;
import com.mohe.spring.entity.RecentView;
import com.mohe.spring.entity.User;
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

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

@Service
@Transactional
public class ActivityService {
    
    private final RecentViewRepository recentViewRepository;
    private final UserRepository userRepository;
    
    public ActivityService(RecentViewRepository recentViewRepository, UserRepository userRepository) {
        this.recentViewRepository = recentViewRepository;
        this.userRepository = userRepository;
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
                    data.setLocation(recentView.getPlace().getLocation());
                    data.setImage(recentView.getPlace().getImageUrl());
                    data.setRating(recentView.getPlace().getRating());
                    data.setViewedAt(recentView.getViewedAt());
                    return data;
                })
                .collect(Collectors.toList());
        
        RecentPlacesResponse response = new RecentPlacesResponse();
        response.setRecentPlaces(recentPlaces);
        
        return response;
    }
    
    public MyPlacesResponse getMyPlaces() {
        // TODO: Implement user-contributed places functionality
        // For now, return empty list
        MyPlacesResponse response = new MyPlacesResponse();
        response.setMyPlaces(Collections.emptyList());
        
        return response;
    }
    
    private User getCurrentUser() {
        UserPrincipal userPrincipal = (UserPrincipal) SecurityContextHolder.getContext().getAuthentication().getPrincipal();
        return userRepository.findById(userPrincipal.getId())
                .orElseThrow(() -> new RuntimeException("사용자를 찾을 수 없습니다"));
    }
}