package com.example.ingestion.controller

import com.example.ingestion.dto.DataReadDto
import com.example.ingestion.dto.DataWriteDto
import com.example.ingestion.dto.PagedDataResponse
import com.example.ingestion.service.DataService
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
    private val dataIngestionJob: Job
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
            
            val jobExecution = jobLauncher.run(dataIngestionJob, jobParametersBuilder.toJobParameters())
            
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
}