package com.mohe.spring.entity

/**
 * Data class representing a selected keyword with confidence score
 * Used in keyword extraction results
 */
data class SelectedKeyword(
    val keywordId: Int,
    val keyword: String,
    val confidence: Double,
    val reasoning: String? = null
)