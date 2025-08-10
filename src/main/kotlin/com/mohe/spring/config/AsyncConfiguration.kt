package com.mohe.spring.config

import org.slf4j.LoggerFactory
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.scheduling.annotation.AsyncConfigurer
import org.springframework.scheduling.annotation.EnableAsync
import org.springframework.scheduling.annotation.EnableScheduling
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor
import java.util.concurrent.Executor
import java.util.concurrent.RejectedExecutionHandler
import java.util.concurrent.ThreadPoolExecutor

/**
 * Configuration for async processing and scheduling
 * Used for similarity calculations and other background tasks
 */
@Configuration
@EnableAsync
@EnableScheduling
class AsyncConfiguration : AsyncConfigurer {

    private val logger = LoggerFactory.getLogger(AsyncConfiguration::class.java)

    /**
     * Configure default async executor with proper settings
     * for similarity calculation tasks
     */
    @Bean("taskExecutor")
    override fun getAsyncExecutor(): Executor {
        val executor = ThreadPoolTaskExecutor()
        
        // Thread pool settings optimized for similarity calculations
        executor.corePoolSize = 4
        executor.maxPoolSize = 8
        executor.queueCapacity = 100
        executor.threadNamePrefix = "MoheAsync-"
        executor.keepAliveSeconds = 60
        
        // Handle rejected tasks gracefully
        executor.setRejectedExecutionHandler(CustomRejectedExecutionHandler())
        
        // Graceful shutdown
        executor.setWaitForTasksToCompleteOnShutdown(true)
        executor.setAwaitTerminationSeconds(60)
        
        executor.initialize()
        
        logger.info("Initialized async task executor with core pool size: ${executor.corePoolSize}, max pool size: ${executor.maxPoolSize}")
        
        return executor
    }

    /**
     * Dedicated executor for similarity calculations
     * Separate from general async tasks to avoid blocking
     */
    @Bean("similarityExecutor")
    fun similarityExecutor(): Executor {
        val executor = ThreadPoolTaskExecutor()
        
        // Conservative settings for CPU-intensive similarity calculations
        executor.corePoolSize = 2
        executor.maxPoolSize = 4
        executor.queueCapacity = 50
        executor.threadNamePrefix = "Similarity-"
        executor.keepAliveSeconds = 300 // Longer keep-alive for stability
        
        executor.setRejectedExecutionHandler(CustomRejectedExecutionHandler())
        executor.setWaitForTasksToCompleteOnShutdown(true)
        executor.setAwaitTerminationSeconds(120) // Longer wait for similarity calculations
        
        executor.initialize()
        
        logger.info("Initialized similarity calculation executor with core pool size: ${executor.corePoolSize}")
        
        return executor
    }

    /**
     * Custom rejection handler that logs rejected tasks
     * instead of throwing exceptions
     */
    inner class CustomRejectedExecutionHandler : RejectedExecutionHandler {
        override fun rejectedExecution(r: Runnable, executor: ThreadPoolExecutor) {
            logger.warn("Task rejected by executor: ${executor.threadNamePrefix} (active: ${executor.activeCount}, pool: ${executor.poolSize}, queue: ${executor.queue.size})")
            
            // Try to execute in caller thread as fallback
            try {
                r.run()
                logger.info("Rejected task executed in caller thread")
            } catch (ex: Exception) {
                logger.error("Failed to execute rejected task in caller thread", ex)
            }
        }
    }
}