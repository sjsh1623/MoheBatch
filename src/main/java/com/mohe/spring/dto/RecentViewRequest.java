package com.mohe.spring.dto;

import jakarta.validation.constraints.NotBlank;

public class RecentViewRequest {

    @NotBlank(message = "장소 ID는 필수입니다")
    private String placeId;

    public RecentViewRequest() {
    }

    public RecentViewRequest(String placeId) {
        this.placeId = placeId;
    }

    public String getPlaceId() {
        return placeId;
    }

    public void setPlaceId(String placeId) {
        this.placeId = placeId;
    }
}
