package com.mohe.spring.dto;

import jakarta.validation.constraints.NotBlank;

public class BookmarkToggleRequest {
    @NotBlank(message = "장소 ID는 필수입니다")
    private String placeId;

    public BookmarkToggleRequest() {}

    public BookmarkToggleRequest(String placeId) {
        this.placeId = placeId;
    }

    public String getPlaceId() {
        return placeId;
    }

    public void setPlaceId(String placeId) {
        this.placeId = placeId;
    }
}