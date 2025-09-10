package com.example.ingestion.controller

import com.example.ingestion.service.MockPlaceService
import org.slf4j.LoggerFactory
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.*

/**
 * TEMPORARY CONTROLLER FOR DEVELOPMENT PURPOSES ONLY
 * 
 * This controller bypasses the Naver API authentication issues
 * and directly inserts mock place data into the database.
 * This is meant to help increase the places count from 14293.
 * 
 * Remove this controller once the API authentication is fixed.
 */
@RestController
@RequestMapping("/api/temp/places")
class TemporaryPlaceController(
    private val mockPlaceService: MockPlaceService
) {
    
    private val logger = LoggerFactory.getLogger(TemporaryPlaceController::class.java)
    
    @PostMapping("/insert-mock")
    fun insertMockPlaces(@RequestParam(defaultValue = "20") count: Int): ResponseEntity<Map<String, Any>> {
        logger.info("Request to insert {} mock places", count)
        
        return try {
            val insertedIds = mockPlaceService.insertMockPlaces(count)
            val summary = mockPlaceService.getMockPlacesSummary()
            
            val response: Map<String, Any> = mapOf(
                "success" to true,
                "message" to "Successfully attempted to insert $count mock places",
                "attempted_count" to count,
                "inserted_ids_count" to insertedIds.size,
                "current_total_count" to (summary["total_places_count"] ?: 0),
                "timestamp" to System.currentTimeMillis()
            )
            
            logger.info("Mock place insertion completed. Total places now: {}", summary["total_places_count"])
            ResponseEntity.ok(response)
            
        } catch (e: Exception) {
            logger.error("Failed to insert mock places", e)
            val errorResponse: Map<String, Any> = mapOf(
                "success" to false,
                "message" to "Failed to insert mock places: ${e.message}",
                "error" to e.javaClass.simpleName,
                "timestamp" to System.currentTimeMillis()
            )
            ResponseEntity.internalServerError().body(errorResponse)
        }
    }
    
    @PostMapping("/insert-batch")
    fun insertBatchMockPlaces(
        @RequestParam(defaultValue = "100") totalCount: Int,
        @RequestParam(defaultValue = "20") batchSize: Int
    ): ResponseEntity<Map<String, Any>> {
        logger.info("Request to insert {} mock places in batches of {}", totalCount, batchSize)
        
        return try {
            val batches = (totalCount + batchSize - 1) / batchSize
            var totalInserted = 0
            
            repeat(batches) { batchIndex ->
                val currentBatchSize = if (batchIndex == batches - 1) {
                    totalCount - (batchIndex * batchSize)
                } else {
                    batchSize
                }
                
                logger.info("Inserting batch {} of {} (size: {})", batchIndex + 1, batches, currentBatchSize)
                val inserted = mockPlaceService.insertMockPlaces(currentBatchSize)
                totalInserted += inserted.size
                
                // Small delay between batches to avoid overwhelming the database
                Thread.sleep(100)
            }
            
            val summary = mockPlaceService.getMockPlacesSummary()
            
            val response: Map<String, Any> = mapOf(
                "success" to true,
                "message" to "Successfully completed batch insertion",
                "requested_total_count" to totalCount,
                "batch_size" to batchSize,
                "total_batches" to batches,
                "total_inserted_count" to totalInserted,
                "current_total_count" to (summary["total_places_count"] ?: 0),
                "timestamp" to System.currentTimeMillis()
            )
            
            logger.info("Batch insertion completed. Total places now: {}", summary["total_places_count"])
            ResponseEntity.ok(response)
            
        } catch (e: Exception) {
            logger.error("Failed to complete batch insertion", e)
            val errorResponse: Map<String, Any> = mapOf(
                "success" to false,
                "message" to "Failed to complete batch insertion: ${e.message}",
                "error" to e.javaClass.simpleName,
                "timestamp" to System.currentTimeMillis()
            )
            ResponseEntity.internalServerError().body(errorResponse)
        }
    }
    
    @GetMapping("/summary")
    fun getPlacesSummary(): ResponseEntity<Map<String, Any>> {
        return try {
            val summary = mockPlaceService.getMockPlacesSummary()
            logger.info("Places summary requested. Current count: {}", summary["total_places_count"])
            ResponseEntity.ok(summary)
        } catch (e: Exception) {
            logger.error("Failed to get places summary", e)
            val errorResponse: Map<String, Any> = mapOf(
                "success" to false,
                "message" to "Failed to get places summary: ${e.message}",
                "error" to e.javaClass.simpleName,
                "timestamp" to System.currentTimeMillis()
            )
            ResponseEntity.internalServerError().body(errorResponse)
        }
    }
    
    @PostMapping("/continuous-insert")
    fun startContinuousInsert(
        @RequestParam(defaultValue = "10") batchSize: Int,
        @RequestParam(defaultValue = "5000") intervalMs: Long,
        @RequestParam(defaultValue = "5") totalBatches: Int
    ): ResponseEntity<Map<String, Any>> {
        logger.info("Starting continuous insert: {} batches of {} places every {}ms", totalBatches, batchSize, intervalMs)
        
        return try {
            // Run in a separate thread to avoid blocking
            Thread {
                repeat(totalBatches) { batchIndex ->
                    try {
                        logger.info("Continuous insert batch {} of {}", batchIndex + 1, totalBatches)
                        mockPlaceService.insertMockPlaces(batchSize)
                        
                        if (batchIndex < totalBatches - 1) {
                            Thread.sleep(intervalMs)
                        }
                    } catch (e: Exception) {
                        logger.error("Error in continuous insert batch {}: {}", batchIndex + 1, e.message)
                    }
                }
                logger.info("Continuous insert completed after {} batches", totalBatches)
            }.start()
            
            val response: Map<String, Any> = mapOf(
                "success" to true,
                "message" to "Continuous insert started in background",
                "batch_size" to batchSize,
                "interval_ms" to intervalMs,
                "total_batches" to totalBatches,
                "estimated_duration_ms" to (totalBatches - 1) * intervalMs,
                "timestamp" to System.currentTimeMillis()
            )
            
            ResponseEntity.ok(response)
            
        } catch (e: Exception) {
            logger.error("Failed to start continuous insert", e)
            val errorResponse: Map<String, Any> = mapOf(
                "success" to false,
                "message" to "Failed to start continuous insert: ${e.message}",
                "error" to e.javaClass.simpleName,
                "timestamp" to System.currentTimeMillis()
            )
            ResponseEntity.internalServerError().body(errorResponse)
        }
    }
}