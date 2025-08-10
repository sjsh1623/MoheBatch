package com.example.ingestion.dto

import com.fasterxml.jackson.annotation.JsonProperty
import java.time.OffsetDateTime

/**
 * Response from external API - represents paginated data
 */
data class ExternalApiResponse(
    @JsonProperty("data")
    val data: List<ExternalDataItem>,
    
    @JsonProperty("pagination")
    val pagination: PaginationInfo
)

data class PaginationInfo(
    @JsonProperty("current_page")
    val currentPage: Int,
    
    @JsonProperty("total_pages")
    val totalPages: Int,
    
    @JsonProperty("page_size")
    val pageSize: Int,
    
    @JsonProperty("total_items")
    val totalItems: Long,
    
    @JsonProperty("has_next")
    val hasNext: Boolean
)

/**
 * Individual data item from external API
 */
data class ExternalDataItem(
    @JsonProperty("id")
    val id: String,
    
    @JsonProperty("name")
    val name: String?,
    
    @JsonProperty("email")
    val email: String?,
    
    @JsonProperty("phone")
    val phone: String?,
    
    @JsonProperty("department")
    val department: String?,
    
    @JsonProperty("status")
    val status: String?,
    
    @JsonProperty("created_at")
    val createdAt: OffsetDateTime?,
    
    @JsonProperty("updated_at")
    val updatedAt: OffsetDateTime?
)