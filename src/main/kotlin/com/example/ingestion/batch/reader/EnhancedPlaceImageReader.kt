package com.example.ingestion.batch.reader

import com.example.ingestion.config.ImageGenerationProperties
import com.example.ingestion.entity.Place
import jakarta.persistence.EntityManager
import jakarta.persistence.EntityManagerFactory
import org.slf4j.LoggerFactory
import org.springframework.batch.core.configuration.annotation.StepScope
import org.springframework.batch.item.ItemReader
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Component
import java.io.File
import java.time.LocalDate

@Component
@StepScope
class EnhancedPlaceImageReader(
    private val entityManagerFactory: EntityManagerFactory,
    private val imageProperties: ImageGenerationProperties,
    @Value("#{jobParameters['since']}") private val since: String?,
    @Value("#{jobParameters['limit']}") private val limit: Long?,
    @Value("#{jobParameters['force'] ?: false}") private val force: Boolean
) : ItemReader<Place> {

    private val logger = LoggerFactory.getLogger(javaClass)
    private var entityManager: EntityManager? = null
    private var places: MutableList<Place>? = null
    private var currentIndex = 0
    
    override fun read(): Place? {
        if (places == null) {
            initializePlaces()
        }
        
        return if (currentIndex < places!!.size) {
            places!![currentIndex++]
        } else {
            null
        }
    }
    
    private fun initializePlaces() {
        entityManager = entityManagerFactory.createEntityManager()
        
        val queryParams = mutableMapOf<String, Any>()
        var queryString = if (force) {
            // If force=true, process all places
            "SELECT p FROM Place p"
        } else {
            // Normal mode: only places without image or with missing files
            "SELECT p FROM Place p WHERE p.imageUrl IS NULL OR p.imageUrl = ''"
        }
        
        // Add date filter if provided
        since?.let {
            val startDate = LocalDate.parse(it).atStartOfDay()
            queryString += if (queryString.contains("WHERE")) " AND" else " WHERE"
            queryString += " p.updatedAt >= :since"
            queryParams["since"] = startDate
        }
        
        queryString += " ORDER BY p.id"
        
        logger.info("Executing query: {} with params: {}", queryString, queryParams)
        
        val query = entityManager!!.createQuery(queryString, Place::class.java)
        queryParams.forEach { (key, value) -> query.setParameter(key, value) }
        
        limit?.let { query.maxResults = it.toInt() }
        
        val allPlaces = query.resultList
        logger.info("Found {} total places from query", allPlaces.size)
        
        // Filter places that need processing
        places = if (force) {
            allPlaces.toMutableList()
        } else {
            allPlaces.filter { place ->
                needsImageProcessing(place)
            }.toMutableList()
        }
        
        logger.info("After filtering, {} places need image processing", places!!.size)
    }
    
    /**
     * Check if a place needs image processing
     * - No imageUrl (null or empty)
     * - imageUrl points to missing file
     */
    private fun needsImageProcessing(place: Place): Boolean {
        // If no imageUrl, definitely needs processing
        if (place.imageUrl.isNullOrBlank()) {
            return true
        }
        
        // Check if file exists on disk
        val relativePath = place.imageUrl!!.removePrefix("/")
        val imageFile = File(imageProperties.publish.rootDir, relativePath)
        
        if (!imageFile.exists()) {
            logger.debug("Image file missing for place {}: {}", place.id, imageFile.absolutePath)
            return true
        }
        
        return false
    }
    
    fun close() {
        entityManager?.close()
    }
}