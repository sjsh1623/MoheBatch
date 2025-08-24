package com.example.ingestion.config

import org.slf4j.LoggerFactory
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.scheduling.annotation.EnableAsync
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor
import java.util.concurrent.Executor

@Configuration
@EnableAsync
class AsyncConfiguration {
    
    private val logger = LoggerFactory.getLogger(AsyncConfiguration::class.java)
    
    @Bean("batchAsyncExecutor")
    fun batchAsyncExecutor(): Executor {
        val executor = ThreadPoolTaskExecutor()
        executor.corePoolSize = 2
        executor.maxPoolSize = 4
        executor.queueCapacity = 10
        executor.setThreadNamePrefix("batch-async-")
        executor.setWaitForTasksToCompleteOnShutdown(true)
        executor.setAwaitTerminationSeconds(30)
        executor.initialize()
        
        logger.info("Configured batch async executor: corePoolSize=2, maxPoolSize=4, queueCapacity=10")
        return executor
    }
}