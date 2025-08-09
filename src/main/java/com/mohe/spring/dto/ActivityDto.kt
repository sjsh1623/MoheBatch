package com.mohe.spring.dto

import java.math.BigDecimal
import java.time.OffsetDateTime

// Recent Places
data class RecentPlacesResponse(
    val recentPlaces: List<RecentPlaceData>
)

data class RecentPlaceData(
    val id: String,
    val title: String,
    val location: String?,
    val image: String?,
    val rating: BigDecimal,
    val viewedAt: OffsetDateTime
)

// My Places
data class MyPlacesResponse(
    val myPlaces: List<MyPlaceData>
)

data class MyPlaceData(
    val id: String,
    val title: String,
    val location: String?,
    val image: String?,
    val status: String,
    val createdAt: OffsetDateTime
)