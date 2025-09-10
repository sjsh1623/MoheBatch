package com.example.ingestion.batch.writer

import com.example.ingestion.entity.Place
import com.example.ingestion.repository.PlaceRepository
import org.slf4j.LoggerFactory
import org.springframework.batch.core.annotation.BeforeStep
import org.springframework.batch.item.Chunk
import org.springframework.batch.item.ItemWriter
import org.springframework.stereotype.Component

@Component
class PlaceWriter(
    private val placeRepository: PlaceRepository
) : ItemWriter<Place> {

    private val logger = LoggerFactory.getLogger(PlaceWriter::class.java)
    private var insertedCount = 0
    private var updatedCount = 0

    @BeforeStep
    fun beforeStep() {
        insertedCount = 0
        updatedCount = 0
    }

    override fun write(items: Chunk<out Place>) {
        var successfulUpserts = 0
        var failedUpserts = 0
        
        items.forEach {
            try {
                val rowsAffected = placeRepository.upsert(
                    locatName = it.locatName,
                    sido = it.sido,
                    sigungu = it.sigungu,
                    dong = it.dong,
                    latitude = it.latitude,
                    longitude = it.longitude,
                    naverData = null, // Not available from this source
                    googleData = null // Not available from this source
                )
                if (rowsAffected > 0) {
                    insertedCount++ // Cumulative successful upserts
                    successfulUpserts++ // This chunk only
                } else {
                    failedUpserts++
                    logger.warn("Upsert returned 0 rows for: ${it.sido} ${it.sigungu} ${it.dong}")
                }
            } catch (e: Exception) {
                failedUpserts++
                logger.error("Failed to upsert place: ${it.sido} ${it.sigungu} ${it.dong} - Error: ${e.message}")
            }
        }
        
        logger.info("Chunk result: ${successfulUpserts} successful upserts, ${failedUpserts} failed | Cumulative total: ${insertedCount} | Chunk size: ${items.size()}")
    }
}
