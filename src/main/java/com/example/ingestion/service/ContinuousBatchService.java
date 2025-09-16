package com.example.ingestion.service;

import com.example.ingestion.service.impl.OptimizedContinuousBatchService;

/**
 * Interface for continuous batch processing service
 */
public interface ContinuousBatchService {

    /**
     * Start continuous batch processing
     */
    void startContinuousProcessing();

    /**
     * Stop continuous batch processing
     */
    void stopContinuousProcessing();

    /**
     * Get current service status
     */
    OptimizedContinuousBatchService.ServiceStatus getServiceStatus();
}