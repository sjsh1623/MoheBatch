package com.example.ingestion.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Naver Place API response item
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class NaverPlaceItem {

    @JsonProperty("title")
    private String title;

    @JsonProperty("link")
    private String link;

    @JsonProperty("category")
    private String category;

    @JsonProperty("description")
    private String description;

    @JsonProperty("telephone")
    private String telephone;

    @JsonProperty("address")
    private String address;

    @JsonProperty("roadAddress")
    private String roadAddress;

    @JsonProperty("mapx")
    private String mapx; // longitude * 10,000,000

    @JsonProperty("mapy")
    private String mapy; // latitude * 10,000,000

    // Default constructor
    public NaverPlaceItem() {}

    // Helper methods
    public String getCleanTitle() {
        if (title == null) return "";
        // Remove HTML tags and clean up title
        return title.replaceAll("<[^>]*>", "").trim();
    }

    public Double getLatitude() {
        if (mapy == null || mapy.trim().isEmpty()) return null;
        try {
            return Double.parseDouble(mapy) / 10000000.0;
        } catch (NumberFormatException e) {
            return null;
        }
    }

    public Double getLongitude() {
        if (mapx == null || mapx.trim().isEmpty()) return null;
        try {
            return Double.parseDouble(mapx) / 10000000.0;
        } catch (NumberFormatException e) {
            return null;
        }
    }

    // Getters and Setters
    public String getTitle() { return title; }
    public void setTitle(String title) { this.title = title; }

    public String getLink() { return link; }
    public void setLink(String link) { this.link = link; }

    public String getCategory() { return category; }
    public void setCategory(String category) { this.category = category; }

    public String getDescription() { return description; }
    public void setDescription(String description) { this.description = description; }

    public String getTelephone() { return telephone; }
    public void setTelephone(String telephone) { this.telephone = telephone; }

    public String getAddress() { return address; }
    public void setAddress(String address) { this.address = address; }

    public String getRoadAddress() { return roadAddress; }
    public void setRoadAddress(String roadAddress) { this.roadAddress = roadAddress; }

    public String getMapx() { return mapx; }
    public void setMapx(String mapx) { this.mapx = mapx; }

    public String getMapy() { return mapy; }
    public void setMapy(String mapy) { this.mapy = mapy; }

    @Override
    public String toString() {
        return String.format("NaverPlaceItem{title='%s', category='%s', address='%s'}",
                           getCleanTitle(), category, address);
    }
}