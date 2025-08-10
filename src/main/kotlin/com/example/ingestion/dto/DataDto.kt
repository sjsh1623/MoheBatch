package com.example.ingestion.dto

import com.example.ingestion.entity.DataEntity
import com.fasterxml.jackson.annotation.JsonProperty
import jakarta.validation.constraints.Email
import jakarta.validation.constraints.NotBlank
import jakarta.validation.constraints.Size
import java.time.LocalDateTime
import java.time.OffsetDateTime

/**
 * DTO for reading data through web API
 */
data class DataReadDto(
    @JsonProperty("id")
    val id: Long,
    
    @JsonProperty("external_id")
    val externalId: String,
    
    @JsonProperty("name")
    val name: String,
    
    @JsonProperty("email")
    val email: String,
    
    @JsonProperty("phone")
    val phone: String?,
    
    @JsonProperty("department")
    val department: String?,
    
    @JsonProperty("status")
    val status: String?,
    
    @JsonProperty("external_created_at")
    val externalCreatedAt: OffsetDateTime,
    
    @JsonProperty("external_updated_at")
    val externalUpdatedAt: OffsetDateTime,
    
    @JsonProperty("created_at")
    val createdAt: LocalDateTime,
    
    @JsonProperty("updated_at")
    val updatedAt: LocalDateTime
) {
    companion object {
        fun fromEntity(entity: DataEntity): DataReadDto {
            return DataReadDto(
                id = entity.id!!,
                externalId = entity.externalId,
                name = entity.name,
                email = entity.email,
                phone = entity.phone,
                department = entity.department,
                status = entity.status,
                externalCreatedAt = entity.externalCreatedAt,
                externalUpdatedAt = entity.externalUpdatedAt,
                createdAt = entity.createdAt,
                updatedAt = entity.updatedAt
            )
        }
    }
}

/**
 * DTO for creating/updating data through web API
 */
data class DataWriteDto(
    @field:NotBlank(message = "External ID is required")
    @field:Size(max = 100, message = "External ID must not exceed 100 characters")
    @JsonProperty("external_id")
    val externalId: String,
    
    @field:NotBlank(message = "Name is required")
    @field:Size(max = 255, message = "Name must not exceed 255 characters")
    @JsonProperty("name")
    val name: String,
    
    @field:NotBlank(message = "Email is required")
    @field:Email(message = "Email must be valid")
    @field:Size(max = 255, message = "Email must not exceed 255 characters")
    @JsonProperty("email")
    val email: String,
    
    @field:Size(max = 50, message = "Phone must not exceed 50 characters")
    @JsonProperty("phone")
    val phone: String? = null,
    
    @field:Size(max = 100, message = "Department must not exceed 100 characters")
    @JsonProperty("department")
    val department: String? = null,
    
    @field:Size(max = 50, message = "Status must not exceed 50 characters")
    @JsonProperty("status")
    val status: String? = null,
    
    @JsonProperty("external_created_at")
    val externalCreatedAt: OffsetDateTime,
    
    @JsonProperty("external_updated_at")
    val externalUpdatedAt: OffsetDateTime
)

/**
 * Paginated response for data listings
 */
data class PagedDataResponse(
    @JsonProperty("data")
    val data: List<DataReadDto>,
    
    @JsonProperty("pagination")
    val pagination: PageInfo
)

data class PageInfo(
    @JsonProperty("page")
    val page: Int,
    
    @JsonProperty("size")
    val size: Int,
    
    @JsonProperty("total_pages")
    val totalPages: Int,
    
    @JsonProperty("total_elements")
    val totalElements: Long,
    
    @JsonProperty("has_next")
    val hasNext: Boolean,
    
    @JsonProperty("has_previous")
    val hasPrevious: Boolean
)