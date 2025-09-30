package com.mohe.spring.controller;

import com.mohe.spring.dto.ApiResponse;
import com.mohe.spring.dto.TermsListResponse;
import com.mohe.spring.dto.TermsListResponse.TermsDetailResponse;
import com.mohe.spring.dto.TermsListResponse.TermsSummary;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.time.OffsetDateTime;
import java.util.List;

/**
 * Temporary controller that exposes the agreements required during onboarding.
 * The content is intentionally static so that future teams can swap in a
 * database-backed implementation without breaking the client contract.
 */
@RestController
@RequestMapping("/api/terms")
@Tag(name = "Terms", description = "Onboarding agreement APIs")
public class TermsController {

    /**
     * Lists all agreements required during sign-up.
     *
     * @return structured summary for each agreement
     */
    @GetMapping
    @Operation(summary = "List onboarding agreements")
    public ResponseEntity<ApiResponse<TermsListResponse>> getTerms() {
        // Placeholder content that the legal/compliance team can replace later.
        TermsListResponse response = new TermsListResponse(
            List.of(
                new TermsSummary("service-terms", "서비스 이용약관 동의 (필수)", true, true),
                new TermsSummary("privacy-policy", "개인정보 수집 및 이용 동의 (선택)", false, true),
                new TermsSummary("location-terms", "위치 정보 이용약관 동의 (선택)", false, true),
                new TermsSummary("age-verification", "만 14세 이상입니다", true, false)
            )
        );
        return ResponseEntity.ok(ApiResponse.success(response));
    }

    /**
     * Fetches the full text for a single agreement.
     *
     * @param termsId identifier from {@link #getTerms()}
     * @return detailed agreement text so the frontend can display it in a modal
     */
    @GetMapping("/{termsId}")
    @Operation(summary = "Read agreement content")
    public ResponseEntity<ApiResponse<TermsDetailResponse>> getTermsDetail(
            @Parameter(description = "Agreement identifier", example = "service-terms")
            @PathVariable String termsId,
            HttpServletRequest request) {

        // Placeholder body – replace with dynamic content storage as needed.
        TermsDetailResponse detail = new TermsDetailResponse(
            termsId,
            mapTitle(termsId),
            "약관 전문이 아직 등록되지 않았습니다. 운영자가 향후 채워 넣을 수 있도록 엔드포인트만 선언해 둔 상태입니다.",
            OffsetDateTime.now()
        );
        return ResponseEntity.ok(ApiResponse.success(detail));
    }

    private String mapTitle(String id) {
        return switch (id) {
            case "service-terms" -> "서비스 이용약관";
            case "privacy-policy" -> "개인정보 수집 및 이용 동의";
            case "location-terms" -> "위치 정보 이용약관";
            case "age-verification" -> "연령 확인 안내";
            default -> "확인되지 않은 약관";
        };
    }
}
