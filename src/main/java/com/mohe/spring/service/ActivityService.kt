package com.mohe.spring.service

import com.mohe.spring.dto.*
import com.mohe.spring.entity.User
import com.mohe.spring.repository.RecentViewRepository
import com.mohe.spring.repository.UserRepository
import com.mohe.spring.security.UserPrincipal
import org.springframework.data.domain.PageRequest
import org.springframework.data.domain.Sort
import org.springframework.security.core.context.SecurityContextHolder
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional

@Service
@Transactional
class ActivityService(
    private val recentViewRepository: RecentViewRepository,
    private val userRepository: UserRepository
) {
    
    fun getRecentPlaces(): RecentPlacesResponse {
        val currentUser = getCurrentUser()
        val pageable = PageRequest.of(0, 50, Sort.by(Sort.Direction.DESC, "viewedAt"))
        val recentViews = recentViewRepository.findByUserOrderByViewedAtDesc(currentUser, pageable)
        
        val recentPlaces = recentViews.content.map { recentView ->
            RecentPlaceData(
                id = recentView.place.id.toString(),
                title = recentView.place.title,
                location = recentView.place.location,
                image = recentView.place.imageUrl,
                rating = recentView.place.rating,
                viewedAt = recentView.viewedAt
            )
        }
        
        return RecentPlacesResponse(recentPlaces = recentPlaces)
    }
    
    fun getMyPlaces(): MyPlacesResponse {
        // TODO: Implement user-contributed places functionality
        // For now, return empty list
        return MyPlacesResponse(myPlaces = emptyList())
    }
    
    private fun getCurrentUser(): User {
        val userPrincipal = SecurityContextHolder.getContext().authentication.principal as UserPrincipal
        return userRepository.findById(userPrincipal.id)
            .orElseThrow { RuntimeException("사용자를 찾을 수 없습니다") }
    }
}