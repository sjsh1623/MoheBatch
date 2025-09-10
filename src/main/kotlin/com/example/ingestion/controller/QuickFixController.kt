package com.example.ingestion.controller

import com.example.ingestion.service.DirectPlaceInsertionService
import org.springframework.web.bind.annotation.*
import org.slf4j.LoggerFactory

/**
 * Quick Fix Controller - Immediate Solution for Continuous Data Insertion
 * 
 * This controller provides the fastest path to get data flowing into
 * your database without any external API dependencies or batch complexity.
 * Perfect for when you need results NOW.
 */
@RestController
@RequestMapping("/api/quick-fix")
class QuickFixController(
    private val directPlaceInsertionService: DirectPlaceInsertionService
) {
    
    private val logger = LoggerFactory.getLogger(QuickFixController::class.java)

    @PostMapping("/start")
    fun startDataFlow(): Map<String, Any> {
        logger.info("🚀 QUICK FIX - STARTING IMMEDIATE CONTINUOUS DATA INSERTION")
        
        return try {
            directPlaceInsertionService.startContinuousInsertion()
        } catch (e: Exception) {
            logger.error("❌ Quick fix failed: {}", e.message, e)
            mapOf(
                "success" to false,
                "error" to (e.message ?: "Unknown error"),
                "timestamp" to System.currentTimeMillis()
            )
        }
    }

    @PostMapping("/stop")
    fun stopDataFlow(): Map<String, Any> {
        logger.info("⏹️ QUICK FIX - STOPPING CONTINUOUS DATA INSERTION")
        
        return try {
            directPlaceInsertionService.stopContinuousInsertion()
        } catch (e: Exception) {
            logger.error("❌ Quick fix stop failed: {}", e.message, e)
            mapOf(
                "success" to false,
                "error" to (e.message ?: "Unknown error"),
                "timestamp" to System.currentTimeMillis()
            )
        }
    }

    @GetMapping("/status")
    fun getStatus(): Map<String, Any> {
        return directPlaceInsertionService.getStatus()
    }
    
    @GetMapping("/info")
    fun getInfo(): Map<String, Any> {
        return mapOf(
            "service" to "Quick Fix Data Flow",
            "description" to "Immediate continuous place insertion bypassing all external APIs",
            "endpoints" to mapOf(
                "start" to "POST /api/quick-fix/start - Begin continuous data insertion",
                "stop" to "POST /api/quick-fix/stop - Stop data insertion",
                "status" to "GET /api/quick-fix/status - Check insertion status"
            ),
            "features" to listOf(
                "Zero external API dependencies",
                "No batch processing complexity", 
                "Direct database transactions",
                "1 place per second insertion rate",
                "Realistic Korean place data with Seoul coordinates",
                "Immediate start - no waiting for credentials or setup"
            ),
            "usage" to "Simply POST to /api/quick-fix/start and watch your database count increase",
            "timestamp" to System.currentTimeMillis()
        )
    }
}