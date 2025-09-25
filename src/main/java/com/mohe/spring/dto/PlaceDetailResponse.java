package com.mohe.spring.dto;

import java.util.List;

public class PlaceDetailResponse {
    private SimplePlaceDto place;
    private List<String> images;
    private boolean isBookmarked;
    private List<SimplePlaceDto> similarPlaces;

    public PlaceDetailResponse() {}

    public PlaceDetailResponse(SimplePlaceDto place, List<String> images, boolean isBookmarked, List<SimplePlaceDto> similarPlaces) {
        this.place = place;
        this.images = images;
        this.isBookmarked = isBookmarked;
        this.similarPlaces = similarPlaces;
    }

    public SimplePlaceDto getPlace() {
        return place;
    }

    public void setPlace(SimplePlaceDto place) {
        this.place = place;
    }

    public List<String> getImages() {
        return images;
    }

    public void setImages(List<String> images) {
        this.images = images;
    }

    public boolean isBookmarked() {
        return isBookmarked;
    }

    public void setBookmarked(boolean bookmarked) {
        isBookmarked = bookmarked;
    }

    public List<SimplePlaceDto> getSimilarPlaces() {
        return similarPlaces;
    }

    public void setSimilarPlaces(List<SimplePlaceDto> similarPlaces) {
        this.similarPlaces = similarPlaces;
    }
}