package com.mohe.spring.service;

import com.mohe.spring.dto.*;
import com.mohe.spring.entity.User;
import com.mohe.spring.repository.UserRepository;
import com.mohe.spring.security.UserPrincipal;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.Collections;

@Service
@Transactional
public class UserService {
    
    private final UserRepository userRepository;
    
    public UserService(UserRepository userRepository) {
        this.userRepository = userRepository;
    }
    
    public UserPreferencesResponse updatePreferences(UserPreferencesRequest request) {
        User currentUser = getCurrentUser();
        
        String mbtiString = null;
        if (request.getMbti() != null) {
            MbtiPreference mbti = request.getMbti();
            mbtiString = mbti.getExtroversion() + mbti.getSensing() + mbti.getThinking() + mbti.getJudging();
        }
        
        currentUser.setMbti(mbtiString != null ? mbtiString : currentUser.getMbti());
        currentUser.setAgeRange(request.getAgeRange() != null ? request.getAgeRange() : currentUser.getAgeRange());
        currentUser.setTransportation(request.getTransportationMethod() != null ? request.getTransportationMethod() : currentUser.getTransportation());
        currentUser.setIsOnboardingCompleted(true);
        
        userRepository.save(currentUser);
        
        // TODO: Save space preferences to preferences table
        
        UserPreferencesData preferencesData = new UserPreferencesData();
        preferencesData.setMbti(currentUser.getMbti());
        preferencesData.setAgeRange(currentUser.getAgeRange());
        preferencesData.setSpacePreferences(request.getSpacePreferences());
        preferencesData.setTransportationMethod(currentUser.getTransportation());
        
        UserPreferencesResponse response = new UserPreferencesResponse();
        response.setPreferences(preferencesData);
        
        return response;
    }
    
    public UserProfileResponse getUserProfile() {
        User currentUser = getCurrentUser();
        
        // TODO: Fetch space preferences from preferences table
        // For now, return empty list
        
        UserProfileData profileData = new UserProfileData();
        profileData.setId(currentUser.getId().toString());
        profileData.setEmail(currentUser.getEmail());
        profileData.setNickname(currentUser.getNickname());
        profileData.setMbti(currentUser.getMbti());
        profileData.setAgeRange(currentUser.getAgeRange());
        profileData.setSpacePreferences(Collections.emptyList());
        profileData.setTransportationMethod(currentUser.getTransportation());
        profileData.setProfileImage(currentUser.getProfileImageUrl());
        profileData.setCreatedAt(currentUser.getCreatedAt());
        
        UserProfileResponse response = new UserProfileResponse();
        response.setUser(profileData);
        
        return response;
    }
    
    public ProfileEditResponse editProfile(ProfileEditRequest request) {
        User currentUser = getCurrentUser();
        
        // Check nickname uniqueness if changed
        if (request.getNickname() != null && !request.getNickname().equals(currentUser.getNickname())) {
            if (userRepository.existsByNickname(request.getNickname())) {
                throw new RuntimeException("이미 사용중인 닉네임입니다");
            }
        }
        
        currentUser.setNickname(request.getNickname() != null ? request.getNickname() : currentUser.getNickname());
        currentUser.setProfileImageUrl(request.getProfileImage() != null ? request.getProfileImage() : currentUser.getProfileImageUrl());
        
        User savedUser = userRepository.save(currentUser);
        
        ProfileEditData editData = new ProfileEditData();
        editData.setId(savedUser.getId().toString());
        editData.setNickname(savedUser.getNickname());
        editData.setProfileImage(savedUser.getProfileImageUrl());
        
        ProfileEditResponse response = new ProfileEditResponse();
        response.setUser(editData);
        
        return response;
    }
    
    private User getCurrentUser() {
        UserPrincipal userPrincipal = (UserPrincipal) SecurityContextHolder.getContext().getAuthentication().getPrincipal();
        return userRepository.findById(userPrincipal.getId())
                .orElseThrow(() -> new RuntimeException("사용자를 찾을 수 없습니다"));
    }
}