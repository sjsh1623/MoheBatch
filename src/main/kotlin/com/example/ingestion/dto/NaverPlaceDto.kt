package com.example.ingestion.dto

import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import com.fasterxml.jackson.annotation.JsonProperty
import java.math.BigDecimal

@JsonIgnoreProperties(ignoreUnknown = true)
data class NaverLocalSearchResponse(
    val total: Int,
    val start: Int,
    val display: Int,
    val items: List<NaverPlaceItem>
)

@JsonIgnoreProperties(ignoreUnknown = true)
data class NaverPlaceItem(
    val title: String,
    val link: String?,
    val category: String,
    val description: String?,
    val telephone: String?,
    val address: String,
    val roadAddress: String?,
    @JsonProperty("mapx") val mapX: String,  // longitude * 10^7
    @JsonProperty("mapy") val mapY: String   // latitude * 10^7
) {
    val latitude: BigDecimal
        get() = BigDecimal(mapY).divide(BigDecimal("10000000"))
    
    val longitude: BigDecimal
        get() = BigDecimal(mapX).divide(BigDecimal("10000000"))
    
    val cleanTitle: String
        get() = title.replace("</?b>".toRegex(), "").trim()
}