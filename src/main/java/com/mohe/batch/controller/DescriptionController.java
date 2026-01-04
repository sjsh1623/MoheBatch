package com.mohe.batch.controller;

import com.mohe.batch.dto.ApiResponse;
import com.mohe.batch.repository.PlaceRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.*;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;

/**
 * Description 전용 배치 컨트롤러
 * - 리뷰 데이터만 가지고 OpenAI로 mohe_description 생성
 */
@RestController
@RequestMapping("/batch/description")
public class DescriptionController {

    private static final Logger log = LoggerFactory.getLogger(DescriptionController.class);

    private final JobLauncher jobLauncher;
    private final Job descriptionOnlyJob;
    private final PlaceRepository placeRepository;

    public DescriptionController(
            JobLauncher jobLauncher,
            @Qualifier("descriptionOnlyJob") Job descriptionOnlyJob,
            PlaceRepository placeRepository
    ) {
        this.jobLauncher = jobLauncher;
        this.descriptionOnlyJob = descriptionOnlyJob;
        this.placeRepository = placeRepository;
    }

    /**
     * Description 생성 배치 시작
     * POST /batch/description/start
     */
    @PostMapping("/start")
    public ResponseEntity<ApiResponse<Map<String, Object>>> startDescriptionBatch() {
        log.info("🚀 Description 생성 배치 시작 요청");

        try {
            // 처리 대상 수 확인
            long targetCount = placeRepository.countPlacesForDescriptionGeneration();
            if (targetCount == 0) {
                Map<String, Object> result = new HashMap<>();
                result.put("message", "처리할 대상이 없습니다.");
                result.put("targetCount", 0);
                return ResponseEntity.ok(ApiResponse.success(result));
            }

            JobParameters jobParameters = new JobParametersBuilder()
                    .addString("startTime", LocalDateTime.now().toString())
                    .addLong("targetCount", targetCount)
                    .toJobParameters();

            JobExecution execution = jobLauncher.run(descriptionOnlyJob, jobParameters);

            Map<String, Object> result = new HashMap<>();
            result.put("jobExecutionId", execution.getId());
            result.put("status", execution.getStatus().toString());
            result.put("targetCount", targetCount);
            result.put("startTime", LocalDateTime.now().toString());

            log.info("✅ Description 배치 시작: executionId={}, targetCount={}",
                    execution.getId(), targetCount);

            return ResponseEntity.ok(ApiResponse.success(result));

        } catch (JobExecutionAlreadyRunningException e) {
            log.warn("⚠️ Description 배치가 이미 실행 중입니다.");
            return ResponseEntity.badRequest().body(
                    ApiResponse.error("ALREADY_RUNNING", "Description 배치가 이미 실행 중입니다."));
        } catch (Exception e) {
            log.error("❌ Description 배치 시작 실패: {}", e.getMessage());
            return ResponseEntity.internalServerError().body(
                    ApiResponse.error("START_FAILED", e.getMessage()));
        }
    }

    /**
     * Description 생성 대상 수 조회
     * GET /batch/description/count
     */
    @GetMapping("/count")
    public ResponseEntity<ApiResponse<Map<String, Object>>> getTargetCount() {
        long targetCount = placeRepository.countPlacesForDescriptionGeneration();

        Map<String, Object> result = new HashMap<>();
        result.put("targetCount", targetCount);
        result.put("description", "크롤링 완료 + mohe_description 없음 + 리뷰 있음");

        return ResponseEntity.ok(ApiResponse.success(result));
    }

    /**
     * Description 배치 상태 조회
     * GET /batch/description/status
     */
    @GetMapping("/status")
    public ResponseEntity<ApiResponse<Map<String, Object>>> getStatus() {
        Map<String, Object> result = new HashMap<>();
        result.put("message", "Use /batch/status for job status");
        result.put("targetCount", placeRepository.countPlacesForDescriptionGeneration());
        return ResponseEntity.ok(ApiResponse.success(result));
    }
}
