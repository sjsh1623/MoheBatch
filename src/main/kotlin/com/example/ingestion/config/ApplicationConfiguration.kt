package com.example.ingestion.config

import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import org.slf4j.LoggerFactory
import org.springframework.boot.actuate.autoconfigure.metrics.MeterRegistryCustomizer
import org.springframework.boot.info.BuildProperties
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor
import java.util.concurrent.Executor

@Configuration
class ApplicationConfiguration {

    private val logger = LoggerFactory.getLogger(ApplicationConfiguration::class.java)

    @Bean
    fun meterRegistryCustomizer(buildProperties: BuildProperties?): MeterRegistryCustomizer<MeterRegistry> {
        return MeterRegistryCustomizer { registry ->
            registry.config().commonTags(
                "application", "mohe-batch",
                "version", buildProperties?.version ?: "unknown"
            )
        }
    }

    @Bean
    fun taskExecutor(): Executor {
        val executor = ThreadPoolTaskExecutor()
        executor.corePoolSize = 5
        executor.maxPoolSize = 10
        executor.queueCapacity = 100
        executor.setThreadNamePrefix("batch-async-")
        executor.initialize()
        
        logger.info("Configured ThreadPoolTaskExecutor with core pool size: 5, max pool size: 10")
        return executor
    }

    @Bean
    fun simpleMeterRegistry(): MeterRegistry {
        return SimpleMeterRegistry()
    }
}