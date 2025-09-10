package com.example.ingestion.batch.writer

import com.example.ingestion.entity.Place
import com.example.ingestion.repository.PlaceRepository
import org.slf4j.LoggerFactory
import org.springframework.batch.core.configuration.annotation.StepScope
import org.springframework.batch.item.Chunk
import org.springframework.batch.item.ItemWriter
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Component

@Component
@StepScope
class PlaceImageWriter(
    private val placeRepository: PlaceRepository,
    @Value("#{jobParameters['dryRun'] ?: false}") private val dryRun: Boolean
) : ItemWriter<Place> {

    private val logger = LoggerFactory.getLogger(javaClass)

    override fun write(chunk: Chunk<out Place>) {
        if (dryRun) {
            chunk.items.forEach { 
                logger.info("[Dry Run] Would update place ${it.id} with image URL: ${it.imageUrl}")
            }
        } else {
            placeRepository.saveAll(chunk.items)
        }
    }
}
