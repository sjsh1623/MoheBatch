package com.mohe.spring.dto;

import jakarta.validation.constraints.NotBlank;
import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.util.List;

public class BookmarkDto {

    // Bookmark Toggle
    public record BookmarkToggleRequest(
        @NotBlank(message = "장소 ID는 필수입니다")
        String placeId
    ) {}

    public record BookmarkToggleResponse(
        boolean isBookmarked,
        String message
    ) {}

    // Bookmark List
    public record BookmarkListResponse(
        List<BookmarkData> bookmarks
    ) {}

    public record BookmarkData(
        String id,
        BookmarkPlaceData place,
        OffsetDateTime createdAt
    ) {}

    public record BookmarkPlaceData(
        String id,
        String name,
        String location,
        String image,
        BigDecimal rating
    ) {}
}