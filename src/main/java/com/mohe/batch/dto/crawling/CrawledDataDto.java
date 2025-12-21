package com.mohe.batch.dto.crawling;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;
import lombok.Setter;

import java.util.List;
import java.util.Map;

@Getter
@Setter
public class CrawledDataDto {
    private String name;

    @JsonProperty("review_count")
    private String reviewCount;

    @JsonProperty("business_hours")
    private BusinessHoursDto businessHours;

    @JsonProperty("ai_summary")
    private List<String> aiSummary;

    private String description;

    @JsonProperty("original_description")
    private String originalDescription;

    @JsonProperty("parking_available")
    private boolean parkingAvailable;

    @JsonProperty("pet_friendly")
    private boolean petFriendly;

    @JsonProperty("image_urls")
    private List<String> imageUrls;

    @JsonProperty("sns_urls")
    private Map<String, String> snsUrls;

    private List<String> reviews;
}
