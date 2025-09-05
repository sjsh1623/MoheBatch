package com.mohe.spring.dto;

import java.math.BigDecimal;

public class BookmarkPlaceData {
    private String id;
    private String name;
    private String location;
    private String image;
    private Double rating;

    public BookmarkPlaceData() {}

    public BookmarkPlaceData(String id, String name, String location, String image, Double rating) {
        this.id = id;
        this.name = name;
        this.location = location;
        this.image = image;
        this.rating = rating;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getLocation() {
        return location;
    }

    public void setLocation(String location) {
        this.location = location;
    }

    public String getImage() {
        return image;
    }

    public void setImage(String image) {
        this.image = image;
    }

    public Double getRating() {
        return rating;
    }

    public void setRating(Double rating) {
        this.rating = rating;
    }
    
    // Overload for BigDecimal conversion
    public void setRating(BigDecimal rating) {
        this.rating = rating != null ? rating.doubleValue() : null;
    }
}