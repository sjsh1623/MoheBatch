package com.mohe.batch.config;

import com.mohe.batch.dto.queue.WorkerInfo;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.UUID;

@Configuration
@EnableScheduling
public class QueueWorkerConfig {

    @Value("${queue.worker.id:}")
    private String configuredWorkerId;

    @Value("${queue.worker.threads:2}")
    private int workerThreads;

    @Value("${queue.worker.enabled:true}")
    private boolean workerEnabled;

    @Value("${queue.retry.max-attempts:3}")
    private int maxRetryAttempts;

    @Value("${queue.retry.backoff-multiplier:30}")
    private int backoffMultiplier;

    @Bean
    public WorkerInfo workerInfo() {
        String workerId = configuredWorkerId != null && !configuredWorkerId.isEmpty()
                ? configuredWorkerId
                : UUID.randomUUID().toString().substring(0, 8);

        String hostname;
        try {
            hostname = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            hostname = "unknown-" + UUID.randomUUID().toString().substring(0, 4);
        }

        return new WorkerInfo(workerId, hostname, workerThreads, workerEnabled);
    }

    @Bean
    public QueueRetryConfig queueRetryConfig() {
        return new QueueRetryConfig(maxRetryAttempts, backoffMultiplier);
    }

    public record QueueRetryConfig(int maxAttempts, int backoffMultiplier) {}
}
