package com.example.ingestion

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing
import org.springframework.scheduling.annotation.EnableAsync
import org.springframework.scheduling.annotation.EnableScheduling

@SpringBootApplication
@EnableBatchProcessing
@EnableAsync
@EnableScheduling
class DataIngestionApplication

fun main(args: Array<String>) {
    runApplication<DataIngestionApplication>(*args)
}