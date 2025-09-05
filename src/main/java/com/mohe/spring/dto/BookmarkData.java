package com.mohe.spring.dto;

import java.time.LocalDateTime;
import java.time.OffsetDateTime;

public class BookmarkData {
    private String id;
    private BookmarkPlaceData place;
    private LocalDateTime createdAt;

    public BookmarkData() {}

    public BookmarkData(String id, BookmarkPlaceData place, LocalDateTime createdAt) {
        this.id = id;
        this.place = place;
        this.createdAt = createdAt;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public BookmarkPlaceData getPlace() {
        return place;
    }

    public void setPlace(BookmarkPlaceData place) {
        this.place = place;
    }

    public LocalDateTime getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(LocalDateTime createdAt) {
        this.createdAt = createdAt;
    }
    
    // Overload for OffsetDateTime conversion
    public void setCreatedAt(OffsetDateTime createdAt) {
        this.createdAt = createdAt != null ? createdAt.toLocalDateTime() : null;
    }
}