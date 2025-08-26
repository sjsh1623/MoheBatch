package com.example.ingestion.batch.reader

import com.example.ingestion.dto.GooglePlaceDetail
import com.example.ingestion.dto.NaverPlaceItem
import java.math.BigDecimal

data class EnrichedPlace(
    val naverPlace: NaverPlaceItem,
    val googlePlace: GooglePlaceDetail?,
    val googlePhotoUrl: String?,
    val searchContext: PlaceSearchContext
)

data class PlaceSearchContext(
    val query: String,
    val coordinate: SeoulCoordinate,
    val page: Int
)

data class SeoulCoordinate(
    val lat: BigDecimal,
    val lng: BigDecimal, 
    val radius: Int
)