package com.mohe.spring.dto;

import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.util.List;

public class ActivityDto {

    // Recent Places
    public record RecentPlacesResponse(
        List<RecentPlaceData> recentPlaces
    ) {}

    public record RecentPlaceData(
        String id,
        String title,
        String location,
        String image,
        BigDecimal rating,
        OffsetDateTime viewedAt
    ) {}

    // My Places
    public record MyPlacesResponse(
        List<MyPlaceData> myPlaces
    ) {}

    public record MyPlaceData(
        String id,
        String title,
        String location,
        String image,
        String status,
        OffsetDateTime createdAt
    ) {}
}