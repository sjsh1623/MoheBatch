package com.example.ingestion.batch.processor

import com.example.ingestion.dto.ExternalDataItem
import com.example.ingestion.entity.DataEntity
import io.micrometer.core.instrument.MeterRegistry
import org.slf4j.LoggerFactory
import org.springframework.batch.item.ItemProcessor
import org.springframework.stereotype.Component
import java.time.LocalDateTime
import java.time.OffsetDateTime

@Component
class DataProcessor(
    private val meterRegistry: MeterRegistry
) : ItemProcessor<ExternalDataItem, DataEntity> {

    private val logger = LoggerFactory.getLogger(DataProcessor::class.java)

    companion object {
        private val EMAIL_REGEX = Regex("^[A-Za-z0-9+_.-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$")
        private val VALID_STATUSES = setOf("active", "inactive", "pending", "suspended")
    }

    override fun process(item: ExternalDataItem): DataEntity? {
        return try {
            logger.debug("Processing item with external ID: ${item.id}")
            
            // Validate and normalize the item
            val validatedItem = validateAndNormalize(item)
            
            if (validatedItem == null) {
                logger.warn("Item with external ID ${item.id} failed validation, skipping")
                meterRegistry.counter("batch_items_skipped").increment()
                return null
            }

            // Convert to entity
            val entity = convertToEntity(validatedItem)
            
            logger.debug("Successfully processed item with external ID: ${item.id}")
            meterRegistry.counter("batch_items_processed").increment()
            
            entity
        } catch (ex: Exception) {
            logger.error("Error processing item with external ID ${item.id}: ${ex.message}", ex)
            meterRegistry.counter("batch_items_error").increment()
            throw ex
        }
    }

    private fun validateAndNormalize(item: ExternalDataItem): ExternalDataItem? {
        // Validate required fields
        if (item.id.isBlank()) {
            logger.warn("Item missing external ID")
            return null
        }

        val normalizedName = item.name?.trim()
        if (normalizedName.isNullOrBlank()) {
            logger.warn("Item ${item.id} missing name")
            return null
        }

        val normalizedEmail = item.email?.trim()?.lowercase()
        if (normalizedEmail.isNullOrBlank()) {
            logger.warn("Item ${item.id} missing email")
            return null
        }

        // Validate email format
        if (!EMAIL_REGEX.matches(normalizedEmail)) {
            logger.warn("Item ${item.id} has invalid email format: $normalizedEmail")
            return null
        }

        // Validate timestamps
        if (item.createdAt == null || item.updatedAt == null) {
            logger.warn("Item ${item.id} missing required timestamps")
            return null
        }

        // Normalize and validate status
        val normalizedStatus = item.status?.trim()?.lowercase()
        val validatedStatus = if (normalizedStatus != null && normalizedStatus in VALID_STATUSES) {
            normalizedStatus
        } else {
            "active" // default status
        }

        // Normalize other fields
        val normalizedPhone = item.phone?.trim()?.takeIf { it.isNotBlank() }
        val normalizedDepartment = item.department?.trim()?.takeIf { it.isNotBlank() }

        // Validate phone format if present
        if (normalizedPhone != null && !isValidPhoneFormat(normalizedPhone)) {
            logger.warn("Item ${item.id} has invalid phone format: $normalizedPhone")
        }

        return item.copy(
            name = normalizedName,
            email = normalizedEmail,
            phone = normalizedPhone,
            department = normalizedDepartment,
            status = validatedStatus
        )
    }

    private fun convertToEntity(item: ExternalDataItem): DataEntity {
        val now = LocalDateTime.now()
        
        return DataEntity(
            externalId = item.id,
            name = item.name!!,
            email = item.email!!,
            phone = item.phone,
            department = item.department,
            status = item.status,
            externalCreatedAt = item.createdAt!!,
            externalUpdatedAt = item.updatedAt!!,
            createdAt = now,
            updatedAt = now
        )
    }

    private fun isValidPhoneFormat(phone: String): Boolean {
        // Basic phone validation - adjust regex based on your requirements
        val phoneRegex = Regex("^[+]?[0-9\\s\\-().]{7,20}$")
        return phoneRegex.matches(phone)
    }
}