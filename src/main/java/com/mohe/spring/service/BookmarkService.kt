package com.mohe.spring.service

import com.mohe.spring.dto.*
import com.mohe.spring.entity.Bookmark
import com.mohe.spring.entity.User
import com.mohe.spring.repository.BookmarkRepository
import com.mohe.spring.repository.PlaceRepository
import com.mohe.spring.repository.UserRepository
import com.mohe.spring.security.UserPrincipal
import org.springframework.data.domain.PageRequest
import org.springframework.data.domain.Sort
import org.springframework.security.core.context.SecurityContextHolder
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional

@Service
@Transactional
class BookmarkService(
    private val bookmarkRepository: BookmarkRepository,
    private val placeRepository: PlaceRepository,
    private val userRepository: UserRepository
) {
    
    fun toggleBookmark(request: BookmarkToggleRequest): BookmarkToggleResponse {
        val currentUser = getCurrentUser()
        val placeId = request.placeId.toLong()
        
        val place = placeRepository.findById(placeId)
            .orElseThrow { RuntimeException("장소를 찾을 수 없습니다") }
        
        val existingBookmark = bookmarkRepository.findByUserAndPlace(currentUser, place)
        
        return if (existingBookmark.isPresent) {
            // Remove bookmark
            bookmarkRepository.delete(existingBookmark.get())
            BookmarkToggleResponse(
                isBookmarked = false,
                message = "북마크가 제거되었습니다."
            )
        } else {
            // Add bookmark
            val bookmark = Bookmark(
                user = currentUser,
                place = place
            )
            bookmarkRepository.save(bookmark)
            BookmarkToggleResponse(
                isBookmarked = true,
                message = "북마크가 추가되었습니다."
            )
        }
    }
    
    fun getBookmarks(): BookmarkListResponse {
        val currentUser = getCurrentUser()
        val pageable = PageRequest.of(0, 100, Sort.by(Sort.Direction.DESC, "createdAt"))
        val bookmarks = bookmarkRepository.findByUserOrderByCreatedAtDesc(currentUser, pageable)
        
        val bookmarkData = bookmarks.content.map { bookmark ->
            BookmarkData(
                id = bookmark.id.toString(),
                place = BookmarkPlaceData(
                    id = bookmark.place.id.toString(),
                    name = bookmark.place.name,
                    location = bookmark.place.location,
                    image = bookmark.place.imageUrl,
                    rating = bookmark.place.rating
                ),
                createdAt = bookmark.createdAt
            )
        }
        
        return BookmarkListResponse(bookmarks = bookmarkData)
    }
    
    private fun getCurrentUser(): User {
        val userPrincipal = SecurityContextHolder.getContext().authentication.principal as UserPrincipal
        return userRepository.findById(userPrincipal.id)
            .orElseThrow { RuntimeException("사용자를 찾을 수 없습니다") }
    }
}