package com.mohe.batch.entity;

/**
 * 크롤링 상태
 * - PENDING: 크롤링 대기 중
 * - COMPLETED: 크롤링 완료
 * - FAILED: 크롤링 실패 (재시도 가능)
 * - NOT_FOUND: 장소를 찾을 수 없음 (404, 폐업 등 - 재시도 불필요)
 */
public enum CrawlStatus {
    PENDING,
    COMPLETED,
    FAILED,
    NOT_FOUND
}
