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

    // Builder pattern
    public static Builder builder() {
        return new Builder();
    }

    @Override
    public String toString() {
        return String.format("NaverPlaceItem{title='%s', category='%s', address='%s'}",
                           getCleanTitle(), category, address);
    }

    // Builder class
    public static class Builder {
        private String title;
        private String link;
        private String category;
        private String description;
        private String telephone;
        private String address;
        private String roadAddress;
        private String mapx;
        private String mapy;

        public Builder title(String title) {
            this.title = title;
            return this;
        }

        public Builder link(String link) {
            this.link = link;
            return this;
        }

        public Builder category(String category) {
            this.category = category;
            return this;
        }

        public Builder description(String description) {
            this.description = description;
            return this;
        }

        public Builder telephone(String telephone) {
            this.telephone = telephone;
            return this;
        }

        public Builder address(String address) {
            this.address = address;
            return this;
        }

        public Builder roadAddress(String roadAddress) {
            this.roadAddress = roadAddress;
            return this;
        }

        public Builder latitude(double latitude) {
            this.mapy = String.valueOf((long)(latitude * 10000000.0));
            return this;
        }

        public Builder longitude(double longitude) {
            this.mapx = String.valueOf((long)(longitude * 10000000.0));
            return this;
        }

        public NaverPlaceItem build() {
            NaverPlaceItem item = new NaverPlaceItem();
            item.setTitle(this.title);
            item.setLink(this.link);
            item.setCategory(this.category);
            item.setDescription(this.description);
            item.setTelephone(this.telephone);
            item.setAddress(this.address);
            item.setRoadAddress(this.roadAddress);
            item.setMapx(this.mapx);
            item.setMapy(this.mapy);
            return item;
        }
    }
}