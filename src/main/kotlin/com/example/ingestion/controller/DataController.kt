package com.example.ingestion.controller

import com.example.ingestion.config.ContinuousBatchService
import com.example.ingestion.dto.DataReadDto
import com.example.ingestion.dto.DataWriteDto
import com.example.ingestion.dto.PagedDataResponse
import com.example.ingestion.service.DataService
import com.example.ingestion.batch.processor.RegionalPlaceEnrichmentProcessor
import com.example.ingestion.batch.reader.EnrichedPlace
import com.example.ingestion.dto.NaverPlaceItem
import com.example.ingestion.batch.reader.PlaceSearchContext
import com.example.ingestion.batch.reader.SeoulCoordinate
import java.math.BigDecimal
import io.swagger.v3.oas.annotations.Operation
import io.swagger.v3.oas.annotations.Parameter
import io.swagger.v3.oas.annotations.responses.ApiResponse
import io.swagger.v3.oas.annotations.tags.Tag
import jakarta.validation.Valid
import jakarta.validation.constraints.Min
import org.slf4j.LoggerFactory
import org.springframework.batch.core.Job
import org.springframework.batch.core.JobParameters
import org.springframework.batch.core.JobParametersBuilder
import org.springframework.batch.core.launch.JobLauncher
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.format.annotation.DateTimeFormat
import org.springframework.http.HttpStatus
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.*
import java.time.OffsetDateTime

@RestController
@RequestMapping("/api/data")
@Tag(name = "Data Management", description = "APIs for managing ingested data")
class DataController(
    private val dataService: DataService,
    private val jobLauncher: JobLauncher,
    @Qualifier("regionalPlaceIngestionJob") private val regionalPlaceIngestionJob: Job,
    private val continuousBatchService: ContinuousBatchService?,
    private val regionalPlaceEnrichmentProcessor: RegionalPlaceEnrichmentProcessor
) {

    private val logger = LoggerFactory.getLogger(DataController::class.java)

    @GetMapping
    @Operation(
        summary = "Get all data with pagination",
        description = "Retrieve paginated list of ingested data with optional sorting"
    )
    @ApiResponse(responseCode = "200", description = "Data retrieved successfully")
    fun getAllData(
        @Parameter(description = "Page number (0-based)")
        @RequestParam(defaultValue = "0") @Min(0) page: Int,
        
        @Parameter(description = "Page size")
        @RequestParam(defaultValue = "20") @Min(1) size: Int,
        
        @Parameter(description = "Sort field")
        @RequestParam(defaultValue = "updatedAt") sortBy: String,
        
        @Parameter(description = "Sort direction (asc/desc)")
        @RequestParam(defaultValue = "desc") sortDirection: String
    ): ResponseEntity<PagedDataResponse> {
        logger.info("GET /api/data - page: {}, size: {}, sort: {} {}", page, size, sortBy, sortDirection)
        
        val result = dataService.getAllData(page, size, sortBy, sortDirection)
        return ResponseEntity.ok(result)
    }

    @GetMapping("/{id}")
    @Operation(
        summary = "Get data by ID",
        description = "Retrieve specific data record by internal ID"
    )
    @ApiResponse(responseCode = "200", description = "Data found")
    @ApiResponse(responseCode = "404", description = "Data not found")
    fun getDataById(
        @Parameter(description = "Internal data ID")
        @PathVariable id: Long
    ): ResponseEntity<DataReadDto> {
        logger.info("GET /api/data/{}", id)
        
        val result = dataService.getDataById(id)
        return ResponseEntity.ok(result)
    }

    @GetMapping("/external/{externalId}")
    @Operation(
        summary = "Get data by external ID",
        description = "Retrieve specific data record by external system ID"
    )
    @ApiResponse(responseCode = "200", description = "Data found")
    @ApiResponse(responseCode = "404", description = "Data not found")
    fun getDataByExternalId(
        @Parameter(description = "External system ID")
        @PathVariable externalId: String
    ): ResponseEntity<DataReadDto> {
        logger.info("GET /api/data/external/{}", externalId)
        
        val result = dataService.getDataByExternalId(externalId)
        return ResponseEntity.ok(result)
    }

    @GetMapping("/search/email")
    @Operation(
        summary = "Search data by email",
        description = "Search for data records by email address (case-insensitive partial match)"
    )
    @ApiResponse(responseCode = "200", description = "Search completed")
    fun searchByEmail(
        @Parameter(description = "Email to search for")
        @RequestParam email: String,
        
        @Parameter(description = "Page number (0-based)")
        @RequestParam(defaultValue = "0") @Min(0) page: Int,
        
        @Parameter(description = "Page size")
        @RequestParam(defaultValue = "20") @Min(1) size: Int
    ): ResponseEntity<PagedDataResponse> {
        logger.info("GET /api/data/search/email?email={}&page={}&size={}", email, page, size)
        
        val result = dataService.searchByEmail(email, page, size)
        return ResponseEntity.ok(result)
    }

    @GetMapping("/search/department")
    @Operation(
        summary = "Search data by department",
        description = "Search for data records by department (case-insensitive partial match)"
    )
    @ApiResponse(responseCode = "200", description = "Search completed")
    fun searchByDepartment(
        @Parameter(description = "Department to search for")
        @RequestParam department: String,
        
        @Parameter(description = "Page number (0-based)")
        @RequestParam(defaultValue = "0") @Min(0) page: Int,
        
        @Parameter(description = "Page size")
        @RequestParam(defaultValue = "20") @Min(1) size: Int
    ): ResponseEntity<PagedDataResponse> {
        logger.info("GET /api/data/search/department?department={}&page={}&size={}", department, page, size)
        
        val result = dataService.searchByDepartment(department, page, size)
        return ResponseEntity.ok(result)
    }

    @GetMapping("/status/{status}")
    @Operation(
        summary = "Get data by status",
        description = "Retrieve data records filtered by status"
    )
    @ApiResponse(responseCode = "200", description = "Data retrieved")
    fun getDataByStatus(
        @Parameter(description = "Status to filter by")
        @PathVariable status: String,
        
        @Parameter(description = "Page number (0-based)")
        @RequestParam(defaultValue = "0") @Min(0) page: Int,
        
        @Parameter(description = "Page size")
        @RequestParam(defaultValue = "20") @Min(1) size: Int
    ): ResponseEntity<PagedDataResponse> {
        logger.info("GET /api/data/status/{}?page={}&size={}", status, page, size)
        
        val result = dataService.getDataByStatus(status, page, size)
        return ResponseEntity.ok(result)
    }

    @PostMapping
    @Operation(
        summary = "Create or update data",
        description = "Create new data record or update existing one based on external ID"
    )
    @ApiResponse(responseCode = "200", description = "Data updated")
    @ApiResponse(responseCode = "201", description = "Data created")
    @ApiResponse(responseCode = "400", description = "Invalid data")
    fun createOrUpdateData(
        @Parameter(description = "Data to create or update")
        @Valid @RequestBody writeDto: DataWriteDto
    ): ResponseEntity<DataReadDto> {
        logger.info("POST /api/data - external_id: {}", writeDto.externalId)
        
        val result = dataService.createOrUpdateData(writeDto)
        return ResponseEntity.status(HttpStatus.CREATED).body(result)
    }

    @GetMapping("/summary")
    @Operation(
        summary = "Get data summary",
        description = "Get summary statistics about ingested data"
    )
    @ApiResponse(responseCode = "200", description = "Summary retrieved")
    fun getDataSummary(): ResponseEntity<Map<String, Any>> {
        logger.info("GET /api/data/summary")
        
        val result = dataService.getDataSummary()
        return ResponseEntity.ok(result)
    }

    @GetMapping("/updated-after")
    @Operation(
        summary = "Get data updated after timestamp",
        description = "Retrieve data records that were updated after the specified timestamp"
    )
    @ApiResponse(responseCode = "200", description = "Data retrieved")
    fun getDataUpdatedAfter(
        @Parameter(description = "Timestamp (ISO format)")
        @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) timestamp: OffsetDateTime
    ): ResponseEntity<List<DataReadDto>> {
        logger.info("GET /api/data/updated-after?timestamp={}", timestamp)
        
        val result = dataService.getDataUpdatedAfter(timestamp)
        return ResponseEntity.ok(result)
    }

    @PostMapping("/batch/trigger")
    @Operation(
        summary = "Trigger batch ingestion job",
        description = "Manually trigger the data ingestion batch job"
    )
    @ApiResponse(responseCode = "202", description = "Job triggered successfully")
    @ApiResponse(responseCode = "500", description = "Failed to trigger job")
    fun triggerBatchJob(
        @Parameter(description = "Force restart from page 1")
        @RequestParam(defaultValue = "false") forceRestart: Boolean
    ): ResponseEntity<Map<String, Any>> {
        logger.info("POST /api/data/batch/trigger - forceRestart: {}", forceRestart)
        
        return try {
            val jobParametersBuilder = JobParametersBuilder()
                .addLong("timestamp", System.currentTimeMillis())
                .addString("forceRestart", forceRestart.toString())
            
            val jobExecution = jobLauncher.run(regionalPlaceIngestionJob, jobParametersBuilder.toJobParameters())
            
            val response = mapOf(
                "message" to "Batch job triggered successfully",
                "job_execution_id" to jobExecution.id,
                "job_instance_id" to jobExecution.jobInstance.id,
                "status" to jobExecution.status.toString()
            )
            
            ResponseEntity.status(HttpStatus.ACCEPTED).body(response)
        } catch (ex: Exception) {
            logger.error("Failed to trigger batch job: ${ex.message}", ex)
            
            val errorResponse = mapOf(
                "error" to "Failed to trigger batch job",
                "message" to (ex.message ?: "Unknown error")
            )
            
            ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(errorResponse)
        }
    }

    @PostMapping("/batch/trigger/government-data-ingestion")
    @Operation(
        summary = "Trigger Korean government data ingestion job",
        description = "Manually trigger the Korean government data ingestion batch job"
    )
    @ApiResponse(responseCode = "202", description = "Government data job triggered successfully")
    @ApiResponse(responseCode = "500", description = "Failed to trigger government data job")
    fun triggerGovernmentDataIngestionJob(): ResponseEntity<Map<String, Any>> {
        logger.info("POST /api/data/batch/trigger/government-data-ingestion")
        
        return try {
            val jobParametersBuilder = JobParametersBuilder()
                .addLong("timestamp", System.currentTimeMillis())
            
            val jobExecution = jobLauncher.run(regionalPlaceIngestionJob, jobParametersBuilder.toJobParameters())
            
            val response = mapOf(
                "message" to "Korean government data ingestion job triggered successfully",
                "job_execution_id" to jobExecution.id,
                "job_instance_id" to jobExecution.jobInstance.id,
                "status" to jobExecution.status.toString()
            )
            
            ResponseEntity.status(HttpStatus.ACCEPTED).body(response)
        } catch (ex: Exception) {
            logger.error("Failed to trigger government data ingestion job: ${ex.message}", ex)
            
            val errorResponse = mapOf(
                "error" to "Failed to trigger government data ingestion job",
                "message" to (ex.message ?: "Unknown error")
            )
            
            ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(errorResponse)
        }
    }

    @PostMapping("/batch/continuous/start")
    @Operation(
        summary = "Start continuous batch processing",
        description = "Start continuous batch processing - each batch will trigger the next one immediately after completion"
    )
    @ApiResponse(responseCode = "200", description = "Continuous processing started")
    @ApiResponse(responseCode = "404", description = "Continuous batch service not available")
    fun startContinuousBatch(): ResponseEntity<Map<String, Any>> {
        logger.info("POST /api/data/batch/continuous/start")
        
        return if (continuousBatchService != null) {
            continuousBatchService.startContinuousProcessing()
            
            val response = mapOf(
                "message" to "Continuous batch processing started - no time limits",
                "mode" to "continuous",
                "description" to "Each batch will trigger the next one immediately after completion"
            )
            ResponseEntity.ok(response)
        } else {
            val response = mapOf(
                "error" to "Continuous batch service not available",
                "message" to "Enable with app.batch.continuous.enabled=true"
            )
            ResponseEntity.status(HttpStatus.NOT_FOUND).body(response)
        }
    }

    @PostMapping("/batch/continuous/stop")
    @Operation(
        summary = "Stop continuous batch processing",
        description = "Stop the continuous batch processing loop"
    )
    @ApiResponse(responseCode = "200", description = "Continuous processing stopped")
    @ApiResponse(responseCode = "404", description = "Continuous batch service not available")
    fun stopContinuousBatch(): ResponseEntity<Map<String, Any>> {
        logger.info("POST /api/data/batch/continuous/stop")
        
        return if (continuousBatchService != null) {
            continuousBatchService.stopContinuousProcessing()
            
            val response = mapOf(
                "message" to "Continuous batch processing stopped",
                "mode" to "stopped"
            )
            ResponseEntity.ok(response)
        } else {
            val response = mapOf(
                "error" to "Continuous batch service not available",
                "message" to "Enable with app.batch.continuous.enabled=true"
            )
            ResponseEntity.status(HttpStatus.NOT_FOUND).body(response)
        }
    }

    @GetMapping("/batch/continuous/status")
    @Operation(
        summary = "Get continuous batch processing status",
        description = "Get the current status of continuous batch processing"
    )
    @ApiResponse(responseCode = "200", description = "Status retrieved")
    @ApiResponse(responseCode = "404", description = "Continuous batch service not available")
    fun getContinuousBatchStatus(): ResponseEntity<Map<String, Any>> {
        logger.info("GET /api/data/batch/continuous/status")
        
        return if (continuousBatchService != null) {
            val status = continuousBatchService.getStatus()
            ResponseEntity.ok(status)
        } else {
            val response = mapOf(
                "error" to "Continuous batch service not available",
                "message" to "Enable with app.batch.continuous.enabled=true",
                "isRunning" to false,
                "mode" to "disabled"
            )
            ResponseEntity.status(HttpStatus.NOT_FOUND).body(response)
        }
    }

    @PostMapping("/test/ollama")
    @Operation(
        summary = "Test Ollama integration",
        description = "Test Ollama text generation with mock place data"
    )
    @ApiResponse(responseCode = "200", description = "Ollama test completed")
    fun testOllamaIntegration(): ResponseEntity<Map<String, Any>> {
        logger.info("POST /api/data/test/ollama - Testing Ollama integration")
        
        return try {
            // Create mock place data
            val mockNaverPlace = NaverPlaceItem(
                title = "스타벅스 청담점>>",
                link = "https://store.starbucks.co.kr/",
                category = "카페",
                description = "넓은 공간과 편안한 좌석이 있는 커피전문점",
                telephone = "02-1234-5678",
                address = "서울특별시 강남구 청담동",
                roadAddress = "서울특별시 강남구 청담동로 123",
                mapX = "127.047308",
                mapY = "37.525379"
            )
            
            val searchContext = PlaceSearchContext(
                query = "카페",
                coordinate = SeoulCoordinate(
                    lat = BigDecimal("37.525379"),
                    lng = BigDecimal("127.047308"),
                    radius = 1000
                ),
                page = 1
            )
            
            val enrichedPlace = EnrichedPlace(
                naverPlace = mockNaverPlace,
                googlePlace = null,
                googlePhotoUrl = null,
                searchContext = searchContext
            )
            
            // Process with Ollama
            val startTime = System.currentTimeMillis()
            val processedPlace = regionalPlaceEnrichmentProcessor.process(enrichedPlace)
            val processingTime = System.currentTimeMillis() - startTime
            
            if (processedPlace != null) {
                val response = mapOf(
                    "success" to true,
                    "message" to "Ollama integration test completed successfully",
                    "processing_time_ms" to processingTime,
                    "place_name" to processedPlace.name,
                    "generated_description" to processedPlace.description,
                    "description_length" to processedPlace.description.length,
                    "has_keyword_vector" to processedPlace.keywordVector.isNotEmpty(),
                    "keyword_vector_dimensions" to processedPlace.keywordVector.size,
                    "source_flags" to processedPlace.sourceFlags
                )
                ResponseEntity.ok(response)
            } else {
                val response = mapOf(
                    "success" to false,
                    "message" to "Ollama processing returned null - check logs for details",
                    "processing_time_ms" to processingTime
                )
                ResponseEntity.ok(response)
            }
        } catch (ex: Exception) {
            logger.error("Ollama integration test failed: ${ex.message}", ex)
            
            val errorResponse = mapOf(
                "success" to false,
                "error" to "Ollama integration test failed",
                "message" to (ex.message ?: "Unknown error"),
                "exception_type" to ex.javaClass.simpleName
            )
            
            ResponseEntity.ok(errorResponse)
        }
    }
}