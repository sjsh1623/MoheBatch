package com.example.ingestion.dto

import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import com.fasterxml.jackson.annotation.JsonProperty

@JsonIgnoreProperties(ignoreUnknown = true)
data class GoogleNearbySearchResponse(
    val results: List<GooglePlaceResult>,
    val status: String,
    @JsonProperty("next_page_token") val nextPageToken: String?
)

@JsonIgnoreProperties(ignoreUnknown = true)
data class GoogleTextSearchResponse(
    val results: List<GooglePlaceResult>,
    val status: String,
    @JsonProperty("next_page_token") val nextPageToken: String?
)

@JsonIgnoreProperties(ignoreUnknown = true)
data class GooglePlaceDetailsResponse(
    val result: GooglePlaceDetail?,
    val status: String
)

@JsonIgnoreProperties(ignoreUnknown = true)
data class GooglePlaceResult(
    @JsonProperty("place_id") val placeId: String,
    val name: String,
    val vicinity: String?,
    val rating: Double?,
    @JsonProperty("user_ratings_total") val userRatingsTotal: Int?,
    @JsonProperty("price_level") val priceLevel: Int?,
    val types: List<String>?,
    val photos: List<GooglePhoto>?,
    val geometry: GoogleGeometry?
)

@JsonIgnoreProperties(ignoreUnknown = true)
data class GooglePlaceDetail(
    @JsonProperty("place_id") val placeId: String,
    val name: String,
    @JsonProperty("formatted_address") val formattedAddress: String?,
    @JsonProperty("formatted_phone_number") val formattedPhoneNumber: String?,
    val website: String?,
    @JsonProperty("opening_hours") val openingHours: GoogleOpeningHours?,
    val rating: Double?,
    @JsonProperty("user_ratings_total") val userRatingsTotal: Int?,
    @JsonProperty("price_level") val priceLevel: Int?,
    val types: List<String>?,
    val photos: List<GooglePhoto>?,
    val geometry: GoogleGeometry?,
    val reviews: List<GoogleReview>?
)

@JsonIgnoreProperties(ignoreUnknown = true)
data class GoogleGeometry(
    val location: GoogleLocation
)

@JsonIgnoreProperties(ignoreUnknown = true)
data class GoogleLocation(
    val lat: Double,
    val lng: Double
)

@JsonIgnoreProperties(ignoreUnknown = true)
data class GooglePhoto(
    @JsonProperty("photo_reference") val photoReference: String,
    val height: Int,
    val width: Int,
    @JsonProperty("html_attributions") val htmlAttributions: List<String>?
)

@JsonIgnoreProperties(ignoreUnknown = true)
data class GoogleOpeningHours(
    @JsonProperty("open_now") val openNow: Boolean?,
    val periods: List<GooglePeriod>?,
    @JsonProperty("weekday_text") val weekdayText: List<String>?
)

@JsonIgnoreProperties(ignoreUnknown = true)
data class GooglePeriod(
    val open: GoogleTime?,
    val close: GoogleTime?
)

@JsonIgnoreProperties(ignoreUnknown = true)
data class GoogleTime(
    val day: Int,
    val time: String
)

@JsonIgnoreProperties(ignoreUnknown = true)
data class GoogleReview(
    @JsonProperty("author_name") val authorName: String,
    val rating: Int,
    @JsonProperty("relative_time_description") val relativeTimeDescription: String,
    val text: String
)