package com.mohe.spring.dto;

import java.util.List;

public class PlaceListResponse {
    private List<PlaceDto> places;
    private int totalElements;
    private int totalPages;
    private int currentPage;
    private int size;

    public PlaceListResponse() {}

    public PlaceListResponse(List<PlaceDto> places, int totalElements, int totalPages, int currentPage, int size) {
        this.places = places;
        this.totalElements = totalElements;
        this.totalPages = totalPages;
        this.currentPage = currentPage;
        this.size = size;
    }

    public List<PlaceDto> getPlaces() {
        return places;
    }

    public void setPlaces(List<PlaceDto> places) {
        this.places = places;
    }

    public int getTotalElements() {
        return totalElements;
    }

    public void setTotalElements(int totalElements) {
        this.totalElements = totalElements;
    }

    public int getTotalPages() {
        return totalPages;
    }

    public void setTotalPages(int totalPages) {
        this.totalPages = totalPages;
    }

    public int getCurrentPage() {
        return currentPage;
    }

    public void setCurrentPage(int currentPage) {
        this.currentPage = currentPage;
    }

    public int getSize() {
        return size;
    }

    public void setSize(int size) {
        this.size = size;
    }
}