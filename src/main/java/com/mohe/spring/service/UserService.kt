package com.mohe.spring.service

import com.mohe.spring.dto.*
import com.mohe.spring.entity.User
import com.mohe.spring.repository.UserRepository
import com.mohe.spring.security.UserPrincipal
import org.springframework.security.core.context.SecurityContextHolder
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional

@Service
@Transactional
class UserService(
    private val userRepository: UserRepository
) {
    
    fun updatePreferences(request: UserPreferencesRequest): UserPreferencesResponse {
        val currentUser = getCurrentUser()
        
        val mbtiString = request.mbti?.let { mbti ->
            "${mbti.extroversion}${mbti.sensing}${mbti.thinking}${mbti.judging}"
        }
        
        val updatedUser = currentUser.copy(
            mbti = mbtiString ?: currentUser.mbti,
            ageRange = request.ageRange ?: currentUser.ageRange,
            transportation = request.transportationMethod ?: currentUser.transportation,
            isOnboardingCompleted = true
        )
        
        userRepository.save(updatedUser)
        
        // TODO: Save space preferences to preferences table
        
        return UserPreferencesResponse(
            preferences = UserPreferencesData(
                mbti = updatedUser.mbti,
                ageRange = updatedUser.ageRange,
                spacePreferences = request.spacePreferences,
                transportationMethod = updatedUser.transportation
            )
        )
    }
    
    fun getUserProfile(): UserProfileResponse {
        val currentUser = getCurrentUser()
        
        // TODO: Fetch space preferences from preferences table
        val spacePreferences = emptyList<String>()
        
        return UserProfileResponse(
            user = UserProfileData(
                id = currentUser.id.toString(),
                email = currentUser.email,
                nickname = currentUser.nickname,
                mbti = currentUser.mbti,
                ageRange = currentUser.ageRange,
                spacePreferences = spacePreferences,
                transportationMethod = currentUser.transportation,
                profileImage = currentUser.profileImageUrl,
                createdAt = currentUser.createdAt
            )
        )
    }
    
    fun editProfile(request: ProfileEditRequest): ProfileEditResponse {
        val currentUser = getCurrentUser()
        
        // Check nickname uniqueness if changed
        if (request.nickname != null && request.nickname != currentUser.nickname) {
            if (userRepository.existsByNickname(request.nickname)) {
                throw RuntimeException("이미 사용중인 닉네임입니다")
            }
        }
        
        val updatedUser = currentUser.copy(
            nickname = request.nickname ?: currentUser.nickname,
            profileImageUrl = request.profileImage ?: currentUser.profileImageUrl
        )
        
        val savedUser = userRepository.save(updatedUser)
        
        return ProfileEditResponse(
            user = ProfileEditData(
                id = savedUser.id.toString(),
                nickname = savedUser.nickname,
                profileImage = savedUser.profileImageUrl
            )
        )
    }
    
    private fun getCurrentUser(): User {
        val userPrincipal = SecurityContextHolder.getContext().authentication.principal as UserPrincipal
        return userRepository.findById(userPrincipal.id)
            .orElseThrow { RuntimeException("사용자를 찾을 수 없습니다") }
    }
}