package com.mohe.spring.controller;

import com.mohe.spring.service.EnhancedBatchService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

/**
 * 향상된 배치 처리 컨트롤러
 * 사용자 요구사항에 맞춘 배치 시스템 API
 */
// @RestController - DISABLED: EnhancedBatchService is disabled, so disabling this controller too
@RequestMapping("/api/enhanced-batch")
public class EnhancedBatchController {

    @Autowired
    private EnhancedBatchService enhancedBatchService;

    /**
     * 수동 배치 실행 트리거
     * - 모든 Place 관련 데이터 초기화
     * - 정부 API 기반 동 단위 지역에서 장소 수집
     * - OpenAI Description 생성
     * - Ollama 벡터화
     * - OpenAI 키워드 추출
     * - Gemini 이미지 생성
     */
    @PostMapping("/trigger")
    public ResponseEntity<Map<String, Object>> triggerManualBatch() {
        try {
            Map<String, Object> result = enhancedBatchService.triggerManualBatch();
            return ResponseEntity.ok(result);
        } catch (Exception e) {
            return ResponseEntity.internalServerError().body(Map.of(
                "success", false,
                "error", e.getMessage()
            ));
        }
    }

    /**
     * 배치 상태 조회
     */
    @GetMapping("/status")
    public ResponseEntity<Map<String, Object>> getBatchStatus() {
        try {
            Map<String, Object> status = enhancedBatchService.getBatchStatus();
            return ResponseEntity.ok(status);
        } catch (Exception e) {
            return ResponseEntity.internalServerError().body(Map.of(
                "success", false,
                "error", e.getMessage()
            ));
        }
    }

    /**
     * 배치 중단
     */
    @PostMapping("/stop")
    public ResponseEntity<Map<String, Object>> stopBatch() {
        try {
            enhancedBatchService.stopBatch();
            return ResponseEntity.ok(Map.of(
                "success", true,
                "message", "배치 중단 신호 전송됨"
            ));
        } catch (Exception e) {
            return ResponseEntity.internalServerError().body(Map.of(
                "success", false,
                "error", e.getMessage()
            ));
        }
    }

    /**
     * Place 데이터 초기화
     */
    @PostMapping("/clear-data")
    public ResponseEntity<Map<String, Object>> clearAllPlaceData() {
        try {
            enhancedBatchService.clearAllPlaceData();
            return ResponseEntity.ok(Map.of(
                "success", true,
                "message", "모든 Place 관련 데이터 초기화 완료"
            ));
        } catch (Exception e) {
            return ResponseEntity.internalServerError().body(Map.of(
                "success", false,
                "error", e.getMessage()
            ));
        }
    }
}