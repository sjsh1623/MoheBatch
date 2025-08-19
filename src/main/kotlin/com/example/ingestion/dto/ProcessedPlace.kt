package com.example.ingestion.dto

import java.math.BigDecimal

data class ProcessedPlace(
    val naverPlaceId: String,
    val googlePlaceId: String?,
    val name: String,
    val description: String,
    val category: String,
    val address: String,
    val roadAddress: String?,
    val latitude: BigDecimal,
    val longitude: BigDecimal,
    val phone: String?,
    val websiteUrl: String?,
    val rating: Double?,
    val userRatingsTotal: Int?,
    val priceLevel: Int?,
    val types: List<String>,
    val openingHours: String?, // JSON string
    val imageUrl: String?,
    val sourceFlags: Map<String, Any>,
    val naverRawData: String, // JSON string
    val googleRawData: String?, // JSON string
    val keywordVector: List<Double> = emptyList() // Embedding vector from Ollama
)