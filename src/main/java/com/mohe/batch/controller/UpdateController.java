package com.mohe.batch.controller;

import com.mohe.batch.dto.ApiResponse;
import com.mohe.batch.service.BatchStatusService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.*;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.LocalDateTime;
import java.util.*;

/**
 * 업데이트 전용 배치 컨트롤러
 * - OpenAI 설명 생성 없이 메뉴, 이미지, 리뷰만 업데이트
 */
@RestController
@RequestMapping("/batch/update")
public class UpdateController {

    private static final Logger log = LoggerFactory.getLogger(UpdateController.class);

    private final JobLauncher jobLauncher;
    private final Job updateJob;
    private final BatchStatusService batchStatusService;
    private final int totalWorkers;

    public UpdateController(
            JobLauncher jobLauncher,
            @Qualifier("updateJob") Job updateJob,
            BatchStatusService batchStatusService,
            @Value("${batch.worker.total-workers:3}") int totalWorkers
    ) {
        this.jobLauncher = jobLauncher;
        this.updateJob = updateJob;
        this.batchStatusService = batchStatusService;
        this.totalWorkers = totalWorkers;
    }

    /**
     * 모든 업데이트 실행 (메뉴 + 이미지 + 리뷰)
     * POST /batch/update/start-all
     */
    @PostMapping("/start-all")
    public ResponseEntity<ApiResponse<Map<String, Object>>> startAllUpdates() {
        return startUpdate("all", true, true, true);
    }

    /**
     * 메뉴만 업데이트
     * POST /batch/update/menus
     */
    @PostMapping("/menus")
    public ResponseEntity<ApiResponse<Map<String, Object>>> updateMenus() {
        return startUpdate("menus", true, false, false);
    }

    /**
     * 이미지만 업데이트 (장소 이미지 + 메뉴 이미지)
     * POST /batch/update/images
     */
    @PostMapping("/images")
    public ResponseEntity<ApiResponse<Map<String, Object>>> updateImages() {
        return startUpdate("images", false, true, false);
    }

    /**
     * 리뷰만 업데이트 (중복 제거: 앞 10글자 동일시 skip)
     * POST /batch/update/reviews
     */
    @PostMapping("/reviews")
    public ResponseEntity<ApiResponse<Map<String, Object>>> updateReviews() {
        return startUpdate("reviews", false, false, true);
    }

    /**
     * 메뉴 + 메뉴 이미지 업데이트
     * POST /batch/update/menus-with-images
     */
    @PostMapping("/menus-with-images")
    public ResponseEntity<ApiResponse<Map<String, Object>>> updateMenusWithImages() {
        return startUpdate("menus-images", true, true, false);
    }

    /**
     * 업데이트 작업 시작
     */
    private ResponseEntity<ApiResponse<Map<String, Object>>> startUpdate(
            String updateType,
            boolean updateMenus,
            boolean updateImages,
            boolean updateReviews
    ) {
        log.info("🔄 업데이트 요청: type={}, menus={}, images={}, reviews={}",
                updateType, updateMenus, updateImages, updateReviews);

        Map<String, Object> results = new HashMap<>();
        List<Map<String, Object>> started = new ArrayList<>();
        List<Map<String, Object>> failed = new ArrayList<>();

        for (int workerId = 0; workerId < totalWorkers; workerId++) {
            try {
                JobParameters jobParameters = new JobParametersBuilder()
                        .addLong("workerId", (long) workerId)
                        .addLong("totalWorkers", (long) totalWorkers)
                        .addString("updateType", updateType)
                        .addString("updateMenus", String.valueOf(updateMenus))
                        .addString("updateImages", String.valueOf(updateImages))
                        .addString("updateReviews", String.valueOf(updateReviews))
                        .addString("startTime", LocalDateTime.now().toString())
                        .toJobParameters();

                JobExecution execution = jobLauncher.run(updateJob, jobParameters);

                Map<String, Object> successInfo = new HashMap<>();
                successInfo.put("workerId", workerId);
                successInfo.put("jobExecutionId", execution.getId());
                successInfo.put("status", execution.getStatus().toString());
                started.add(successInfo);

                log.info("✅ Worker {} 업데이트 시작: executionId={}", workerId, execution.getId());

            } catch (JobExecutionAlreadyRunningException e) {
                Map<String, Object> failInfo = new HashMap<>();
                failInfo.put("workerId", workerId);
                failInfo.put("reason", "Already running");
                failed.add(failInfo);
            } catch (Exception e) {
                Map<String, Object> failInfo = new HashMap<>();
                failInfo.put("workerId", workerId);
                failInfo.put("reason", e.getMessage());
                failed.add(failInfo);
                log.error("❌ Worker {} 시작 실패: {}", workerId, e.getMessage());
            }
        }

        results.put("updateType", updateType);
        results.put("started", started);
        results.put("failed", failed);
        results.put("totalWorkers", totalWorkers);

        log.info("🚀 업데이트 시작: {}/{} workers", started.size(), totalWorkers);
        return ResponseEntity.ok(ApiResponse.success(results));
    }

    /**
     * 업데이트 상태 조회
     */
    @GetMapping("/status")
    public ResponseEntity<ApiResponse<Map<String, Object>>> getStatus() {
        Map<String, Object> result = new HashMap<>();
        result.put("message", "Use /batch/status for job status");
        return ResponseEntity.ok(ApiResponse.success(result));
    }
}
