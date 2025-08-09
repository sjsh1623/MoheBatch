package com.mohe.spring.dto

import jakarta.validation.constraints.NotBlank
import java.math.BigDecimal
import java.time.OffsetDateTime

// Bookmark Toggle
data class BookmarkToggleRequest(
    @field:NotBlank(message = "장소 ID는 필수입니다")
    val placeId: String
)

data class BookmarkToggleResponse(
    val isBookmarked: Boolean,
    val message: String
)

// Bookmark List
data class BookmarkListResponse(
    val bookmarks: List<BookmarkData>
)

data class BookmarkData(
    val id: String,
    val place: BookmarkPlaceData,
    val createdAt: OffsetDateTime
)

data class BookmarkPlaceData(
    val id: String,
    val name: String,
    val location: String?,
    val image: String?,
    val rating: BigDecimal
)