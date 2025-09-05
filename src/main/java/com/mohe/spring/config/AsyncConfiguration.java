package com.mohe.spring.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.AsyncConfigurer;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * Configuration for async processing and scheduling
 * Used for similarity calculations and other background tasks
 */
@Configuration
@EnableAsync
@EnableScheduling
public class AsyncConfiguration implements AsyncConfigurer {

    private static final Logger logger = LoggerFactory.getLogger(AsyncConfiguration.class);

    /**
     * Configure default async executor with proper settings
     * for similarity calculation tasks
     */
    @Bean("taskExecutor")
    @Override
    public Executor getAsyncExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        
        // Thread pool settings optimized for similarity calculations
        executor.setCorePoolSize(4);
        executor.setMaxPoolSize(8);
        executor.setQueueCapacity(100);
        executor.setThreadNamePrefix("MoheAsync-");
        executor.setKeepAliveSeconds(60);
        
        // Handle rejected tasks gracefully
        executor.setRejectedExecutionHandler(new CustomRejectedExecutionHandler());
        
        // Graceful shutdown
        executor.setWaitForTasksToCompleteOnShutdown(true);
        executor.setAwaitTerminationSeconds(60);
        
        executor.initialize();
        
        logger.info("Initialized async task executor with core pool size: {}, max pool size: {}", 
                   executor.getCorePoolSize(), executor.getMaxPoolSize());
        
        return executor;
    }

    /**
     * Dedicated executor for similarity calculations
     * Separate from general async tasks to avoid blocking
     */
    @Bean("similarityExecutor")
    public Executor similarityExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        
        // Conservative settings for CPU-intensive similarity calculations
        executor.setCorePoolSize(2);
        executor.setMaxPoolSize(4);
        executor.setQueueCapacity(50);
        executor.setThreadNamePrefix("Similarity-");
        executor.setKeepAliveSeconds(300); // Longer keep-alive for stability
        
        executor.setRejectedExecutionHandler(new CustomRejectedExecutionHandler());
        executor.setWaitForTasksToCompleteOnShutdown(true);
        executor.setAwaitTerminationSeconds(120); // Longer wait for similarity calculations
        
        executor.initialize();
        
        logger.info("Initialized similarity calculation executor with core pool size: {}", 
                   executor.getCorePoolSize());
        
        return executor;
    }

    /**
     * Custom rejection handler that logs rejected tasks
     * instead of throwing exceptions
     */
    public class CustomRejectedExecutionHandler implements RejectedExecutionHandler {
        @Override
        public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
            logger.warn("Task rejected by executor (active: {}, pool: {}, queue: {})", 
                       executor.getActiveCount(), executor.getPoolSize(), executor.getQueue().size());
            
            // Try to execute in caller thread as fallback
            try {
                r.run();
                logger.info("Rejected task executed in caller thread");
            } catch (Exception ex) {
                logger.error("Failed to execute rejected task in caller thread", ex);
            }
        }
    }
}