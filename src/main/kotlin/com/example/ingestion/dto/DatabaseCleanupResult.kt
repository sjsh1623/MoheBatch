package com.example.ingestion.dto

data class DatabaseCleanupResult(
    val removedCount: Int,
    val messages: List<String>
)