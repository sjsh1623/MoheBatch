package com.mohe.spring.controller;

import com.mohe.spring.dto.ApiResponse;
import com.mohe.spring.service.EmailService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.ExampleObject;
// Import removed - using fully qualified name for ApiResponse to avoid conflict
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.security.SecurityRequirements;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.validation.Valid;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.*;

import java.time.LocalDateTime;

@RestController
@RequestMapping("/api/email")
@SecurityRequirements
@Tag(name = "이메일 발송", description = "HTML/텍스트 이메일 발송 API")
public class EmailController {
    
    private static final Logger logger = LoggerFactory.getLogger(EmailController.class);
    private final EmailService emailService;

    public EmailController(EmailService emailService) {
        this.emailService = emailService;
    }

    @PostMapping("/send")
    @Operation(
        summary = "HTML/텍스트 이메일 발송",
        description = """
            HTML 또는 텍스트 이메일을 발송합니다.
            - HTML 콘텐츠는 XSS 방지를 위해 자동 sanitization 처리됩니다
            - 관리자 권한이 필요합니다
            - Gmail SMTP를 사용하여 전송됩니다
        """
    )
    @ApiResponses(
        value = {
            @io.swagger.v3.oas.annotations.responses.ApiResponse(
                responseCode = "200",
                description = "이메일 발송 성공",
                content = @Content(
                    mediaType = "application/json",
                    examples = @ExampleObject(
                        value = """
                        {
                          "success": true,
                          "data": {
                            "message": "이메일이 성공적으로 발송되었습니다",
                            "to": "user@example.com",
                            "subject": "테스트 이메일",
                            "type": "html"
                          }
                        }
                        """
                    )
                )
            ),
            @io.swagger.v3.oas.annotations.responses.ApiResponse(
                responseCode = "400",
                description = "잘못된 요청 (이메일 주소 형식 오류 등)"
            ),
            @io.swagger.v3.oas.annotations.responses.ApiResponse(
                responseCode = "403",
                description = "권한 없음 (관리자 권한 필요)"
            ),
            @io.swagger.v3.oas.annotations.responses.ApiResponse(
                responseCode = "500",
                description = "이메일 발송 실패"
            )
        }
    )
    @PreAuthorize("hasRole('ADMIN')")
    public ResponseEntity<ApiResponse<EmailSendResponse>> sendEmail(
            @Parameter(description = "이메일 발송 요청", required = true)
            @Valid @RequestBody EmailSendRequest request,
            HttpServletRequest httpRequest) {
        try {
            // Validate input
            if (request.getTo() == null || request.getTo().isBlank()) {
                return ResponseEntity.badRequest().body(
                    ApiResponse.error(
                        "INVALID_EMAIL",
                        "수신자 이메일 주소는 필수입니다",
                        httpRequest.getRequestURI()
                    )
                );
            }

            if (request.getSubject() == null || request.getSubject().isBlank()) {
                return ResponseEntity.badRequest().body(
                    ApiResponse.error(
                        "INVALID_SUBJECT",
                        "이메일 제목은 필수입니다",
                        httpRequest.getRequestURI()
                    )
                );
            }

            if (request.getContent() == null || request.getContent().isBlank()) {
                return ResponseEntity.badRequest().body(
                    ApiResponse.error(
                        "INVALID_CONTENT",
                        "이메일 내용은 필수입니다",
                        httpRequest.getRequestURI()
                    )
                );
            }

            logger.info("Processing email send request: to={}, subject={}, type={}", 
                       request.getTo(), request.getSubject(), request.getType());

            // Send email based on type
            String emailType = request.getType().toLowerCase();
            switch (emailType) {
                case "html" -> emailService.sendHtmlEmail(
                    request.getTo(),
                    request.getSubject(),
                    request.getContent(),
                    request.getFrom()
                );
                case "text" -> emailService.sendTextEmail(
                    request.getTo(),
                    request.getSubject(),
                    request.getContent(),
                    request.getFrom()
                );
                default -> {
                    return ResponseEntity.badRequest().body(
                        ApiResponse.error(
                            "INVALID_TYPE",
                            "이메일 타입은 'html' 또는 'text'여야 합니다",
                            httpRequest.getRequestURI()
                        )
                    );
                }
            }

            EmailSendResponse response = new EmailSendResponse(
                "이메일이 성공적으로 발송되었습니다",
                request.getTo(),
                request.getSubject(),
                request.getType()
            );

            logger.info("Email sent successfully: to={}, subject={}", request.getTo(), request.getSubject());
            return ResponseEntity.ok(ApiResponse.success(response));

        } catch (IllegalArgumentException e) {
            logger.error("Invalid email request: {}", e.getMessage());
            return ResponseEntity.badRequest().body(
                ApiResponse.error(
                    "VALIDATION_ERROR",
                    e.getMessage() != null ? e.getMessage() : "잘못된 요청입니다",
                    httpRequest.getRequestURI()
                )
            );
        } catch (Exception e) {
            logger.error("Failed to send email to {}", request.getTo(), e);
            return ResponseEntity.status(500).body(
                ApiResponse.error(
                    "EMAIL_SEND_ERROR",
                    "이메일 발송에 실패했습니다: " + e.getMessage(),
                    httpRequest.getRequestURI()
                )
            );
        }
    }

    @PostMapping("/test")
    @Operation(
        summary = "테스트 이메일 발송",
        description = "관리자용 테스트 이메일을 발송합니다. 개발/테스트 환경에서 이메일 설정을 확인할 때 사용합니다."
    )
    @ApiResponses(
        value = {
            @io.swagger.v3.oas.annotations.responses.ApiResponse(
                responseCode = "200",
                description = "테스트 이메일 발송 성공"
            ),
            @io.swagger.v3.oas.annotations.responses.ApiResponse(
                responseCode = "403",
                description = "권한 없음 (관리자 권한 필요)"
            )
        }
    )
    @PreAuthorize("hasRole('ADMIN')")
    public ResponseEntity<ApiResponse<EmailSendResponse>> sendTestEmail(
            @Parameter(description = "테스트 이메일 주소", required = true, example = "admin@example.com")
            @RequestParam String to,
            HttpServletRequest httpRequest) {
        try {
            String testHtmlContent = String.format("""
                <html>
                <body style="font-family: Arial, sans-serif; max-width: 600px; margin: 0 auto; padding: 20px;">
                    <div style="background: linear-gradient(135deg, #667eea 0%%, #764ba2 100%%); padding: 20px; border-radius: 10px; color: white; text-align: center;">
                        <h1 style="margin: 0; font-size: 24px;">MOHE 테스트 이메일</h1>
                    </div>
                    
                    <div style="padding: 30px 0; text-align: center;">
                        <h2 style="color: #333;">이메일 발송 테스트</h2>
                        <p style="font-size: 16px; color: #666;">이 이메일은 MOHE 시스템의 이메일 발송 기능 테스트용입니다.</p>
                        <p style="font-size: 14px; color: #999;">발송 시간: %s</p>
                    </div>
                    
                    <div style="border-top: 1px solid #eee; padding: 20px 0; text-align: center;">
                        <p style="font-size: 12px; color: #999; margin: 0;">MOHE 팀</p>
                    </div>
                </body>
                </html>
                """, LocalDateTime.now());

            emailService.sendHtmlEmail(
                to,
                "[MOHE] 테스트 이메일",
                testHtmlContent,
                null
            );

            EmailSendResponse response = new EmailSendResponse(
                "테스트 이메일이 성공적으로 발송되었습니다",
                to,
                "[MOHE] 테스트 이메일",
                "html"
            );

            logger.info("Test email sent successfully to: {}", to);
            return ResponseEntity.ok(ApiResponse.success(response));

        } catch (Exception e) {
            logger.error("Failed to send test email to {}", to, e);
            return ResponseEntity.status(500).body(
                ApiResponse.error(
                    "EMAIL_SEND_ERROR",
                    "테스트 이메일 발송에 실패했습니다: " + e.getMessage(),
                    httpRequest.getRequestURI()
                )
            );
        }
    }

    /**
     * Email send request
     */
    public static class EmailSendRequest {
        private String to;
        private String subject;
        private String content;
        private String type = "html"; // "html" or "text"
        private String from;

        public EmailSendRequest() {}

        public EmailSendRequest(String to, String subject, String content, String type, String from) {
            this.to = to;
            this.subject = subject;
            this.content = content;
            this.type = type;
            this.from = from;
        }

        public String getTo() { return to; }
        public void setTo(String to) { this.to = to; }
        
        public String getSubject() { return subject; }
        public void setSubject(String subject) { this.subject = subject; }
        
        public String getContent() { return content; }
        public void setContent(String content) { this.content = content; }
        
        public String getType() { return type; }
        public void setType(String type) { this.type = type; }
        
        public String getFrom() { return from; }
        public void setFrom(String from) { this.from = from; }
    }

    /**
     * Email send response
     */
    public static class EmailSendResponse {
        private final String message;
        private final String to;
        private final String subject;
        private final String type;

        public EmailSendResponse(String message, String to, String subject, String type) {
            this.message = message;
            this.to = to;
            this.subject = subject;
            this.type = type;
        }

        public String getMessage() { return message; }
        public String getTo() { return to; }
        public String getSubject() { return subject; }
        public String getType() { return type; }
    }
}