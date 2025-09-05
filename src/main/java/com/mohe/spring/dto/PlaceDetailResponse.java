package com.mohe.spring.dto;

import java.util.List;

public class PlaceDetailResponse {
    private PlaceDto place;
    private List<String> images;
    private boolean isBookmarked;
    private List<PlaceDto> similarPlaces;

    public PlaceDetailResponse() {}

    public PlaceDetailResponse(PlaceDto place, List<String> images, boolean isBookmarked, List<PlaceDto> similarPlaces) {
        this.place = place;
        this.images = images;
        this.isBookmarked = isBookmarked;
        this.similarPlaces = similarPlaces;
    }

    public PlaceDto getPlace() {
        return place;
    }

    public void setPlace(PlaceDto place) {
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

    public List<PlaceDto> getSimilarPlaces() {
        return similarPlaces;
    }

    public void setSimilarPlaces(List<PlaceDto> similarPlaces) {
        this.similarPlaces = similarPlaces;
    }
}