package com.example.ingestion.batch.writer

import com.example.ingestion.entity.DataEntity
import com.example.ingestion.repository.DataRepository
import io.micrometer.core.instrument.MeterRegistry
import org.slf4j.LoggerFactory
import org.springframework.batch.item.Chunk
import org.springframework.batch.item.ItemWriter
import org.springframework.dao.DataIntegrityViolationException
import org.springframework.stereotype.Component
import org.springframework.transaction.annotation.Transactional
import java.time.LocalDateTime

@Component
class DatabaseWriter(
    private val dataRepository: DataRepository,
    private val meterRegistry: MeterRegistry
) : ItemWriter<DataEntity> {

    private val logger = LoggerFactory.getLogger(DatabaseWriter::class.java)

    @Transactional
    override fun write(chunk: Chunk<out DataEntity>) {
        if (chunk.isEmpty()) {
            return
        }

        logger.info("Writing chunk of ${chunk.size()} items to database")
        
        var insertedCount = 0
        var updatedCount = 0
        var errorCount = 0

        chunk.items.forEach { entity ->
            try {
                val result = upsertEntity(entity)
                when (result) {
                    UpsertResult.INSERTED -> insertedCount++
                    UpsertResult.UPDATED -> updatedCount++
                }
            } catch (ex: Exception) {
                logger.error("Failed to upsert entity with external ID ${entity.externalId}: ${ex.message}", ex)
                errorCount++
                meterRegistry.counter("batch_database_write_errors").increment()
                
                // Re-throw for chunk-level error handling
                throw ex
            }
        }

        // Update metrics
        meterRegistry.counter("batch_database_inserts").increment(insertedCount.toDouble())
        meterRegistry.counter("batch_database_updates").increment(updatedCount.toDouble())
        
        logger.info("Successfully wrote chunk: $insertedCount inserts, $updatedCount updates, $errorCount errors")
    }

    private fun upsertEntity(entity: DataEntity): UpsertResult {
        return try {
            // Check if entity already exists
            val existingEntity = dataRepository.findByExternalId(entity.externalId)
            
            if (existingEntity.isPresent) {
                // Update existing entity
                val existing = existingEntity.get()
                
                // Only update if the external updated timestamp is newer
                if (entity.externalUpdatedAt.isAfter(existing.externalUpdatedAt)) {
                    dataRepository.upsertByExternalId(
                        externalId = entity.externalId,
                        name = entity.name,
                        email = entity.email,
                        phone = entity.phone,
                        department = entity.department,
                        status = entity.status,
                        externalCreatedAt = entity.externalCreatedAt,
                        externalUpdatedAt = entity.externalUpdatedAt,
                        createdAt = existing.createdAt, // Keep original creation time
                        updatedAt = LocalDateTime.now()
                    )
                    
                    logger.debug("Updated entity with external ID: ${entity.externalId}")
                    UpsertResult.UPDATED
                } else {
                    logger.debug("Entity with external ID ${entity.externalId} is not newer, skipping update")
                    UpsertResult.SKIPPED
                }
            } else {
                // Insert new entity
                dataRepository.upsertByExternalId(
                    externalId = entity.externalId,
                    name = entity.name,
                    email = entity.email,
                    phone = entity.phone,
                    department = entity.department,
                    status = entity.status,
                    externalCreatedAt = entity.externalCreatedAt,
                    externalUpdatedAt = entity.externalUpdatedAt,
                    createdAt = LocalDateTime.now(),
                    updatedAt = LocalDateTime.now()
                )
                
                logger.debug("Inserted new entity with external ID: ${entity.externalId}")
                UpsertResult.INSERTED
            }
        } catch (ex: DataIntegrityViolationException) {
            logger.warn("Data integrity violation for external ID ${entity.externalId}: ${ex.message}")
            
            // Try to handle common integrity violations
            handleIntegrityViolation(entity, ex)
            UpsertResult.UPDATED
        }
    }

    private fun handleIntegrityViolation(entity: DataEntity, ex: DataIntegrityViolationException) {
        when {
            ex.message?.contains("unique constraint", ignoreCase = true) == true -> {
                logger.info("Handling unique constraint violation for external ID: ${entity.externalId}")
                
                // Force update using the upsert query
                dataRepository.upsertByExternalId(
                    externalId = entity.externalId,
                    name = entity.name,
                    email = entity.email,
                    phone = entity.phone,
                    department = entity.department,
                    status = entity.status,
                    externalCreatedAt = entity.externalCreatedAt,
                    externalUpdatedAt = entity.externalUpdatedAt,
                    createdAt = LocalDateTime.now(),
                    updatedAt = LocalDateTime.now()
                )
            }
            else -> {
                logger.error("Unhandled data integrity violation: ${ex.message}")
                throw ex
            }
        }
    }

    enum class UpsertResult {
        INSERTED,
        UPDATED,
        SKIPPED
    }
}