package com.mohe.spring.controller

import com.mohe.spring.dto.ApiResponse
import com.mohe.spring.service.EmailService
import io.swagger.v3.oas.annotations.Operation
import io.swagger.v3.oas.annotations.Parameter
import io.swagger.v3.oas.annotations.media.Content
import io.swagger.v3.oas.annotations.media.ExampleObject
import io.swagger.v3.oas.annotations.media.Schema
import io.swagger.v3.oas.annotations.responses.ApiResponse as SwaggerApiResponse
import io.swagger.v3.oas.annotations.responses.ApiResponses
import io.swagger.v3.oas.annotations.security.SecurityRequirements
import io.swagger.v3.oas.annotations.tags.Tag
import jakarta.servlet.http.HttpServletRequest
import jakarta.validation.Valid
import org.slf4j.LoggerFactory
import org.springframework.http.ResponseEntity
import org.springframework.security.access.prepost.PreAuthorize
import org.springframework.web.bind.annotation.*

@RestController
@RequestMapping("/api/email")
@SecurityRequirements
@Tag(name = "이메일 발송", description = "HTML/텍스트 이메일 발송 API")
class EmailController(
    private val emailService: EmailService
) {
    private val logger = LoggerFactory.getLogger(EmailController::class.java)

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
        value = [
            SwaggerApiResponse(
                responseCode = "200",
                description = "이메일 발송 성공",
                content = [Content(
                    mediaType = "application/json",
                    examples = [ExampleObject(
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
                    )]
                )]
            ),
            SwaggerApiResponse(
                responseCode = "400",
                description = "잘못된 요청 (이메일 주소 형식 오류 등)"
            ),
            SwaggerApiResponse(
                responseCode = "403",
                description = "권한 없음 (관리자 권한 필요)"
            ),
            SwaggerApiResponse(
                responseCode = "500",
                description = "이메일 발송 실패"
            )
        ]
    )
    @PreAuthorize("hasRole('ADMIN')")
    fun sendEmail(
        @Parameter(description = "이메일 발송 요청", required = true)
        @Valid @RequestBody request: EmailSendRequest,
        httpRequest: HttpServletRequest
    ): ResponseEntity<ApiResponse<EmailSendResponse>> {
        return try {
            // Validate input
            if (request.to.isBlank()) {
                return ResponseEntity.badRequest().body(
                    ApiResponse.error(
                        code = "INVALID_EMAIL",
                        message = "수신자 이메일 주소는 필수입니다",
                        path = httpRequest.requestURI
                    )
                )
            }

            if (request.subject.isBlank()) {
                return ResponseEntity.badRequest().body(
                    ApiResponse.error(
                        code = "INVALID_SUBJECT",
                        message = "이메일 제목은 필수입니다",
                        path = httpRequest.requestURI
                    )
                )
            }

            if (request.content.isBlank()) {
                return ResponseEntity.badRequest().body(
                    ApiResponse.error(
                        code = "INVALID_CONTENT",
                        message = "이메일 내용은 필수입니다",
                        path = httpRequest.requestURI
                    )
                )
            }

            logger.info("Processing email send request: to=${request.to}, subject=${request.subject}, type=${request.type}")

            // Send email based on type
            when (request.type.lowercase()) {
                "html" -> {
                    emailService.sendHtmlEmail(
                        to = request.to,
                        subject = request.subject,
                        htmlContent = request.content,
                        from = request.from
                    )
                }
                "text" -> {
                    emailService.sendTextEmail(
                        to = request.to,
                        subject = request.subject,
                        text = request.content,
                        from = request.from
                    )
                }
                else -> {
                    return ResponseEntity.badRequest().body(
                        ApiResponse.error(
                            code = "INVALID_TYPE",
                            message = "이메일 타입은 'html' 또는 'text'여야 합니다",
                            path = httpRequest.requestURI
                        )
                    )
                }
            }

            val response = EmailSendResponse(
                message = "이메일이 성공적으로 발송되었습니다",
                to = request.to,
                subject = request.subject,
                type = request.type
            )

            logger.info("Email sent successfully: to=${request.to}, subject=${request.subject}")
            ResponseEntity.ok(ApiResponse.success(response))

        } catch (e: IllegalArgumentException) {
            logger.error("Invalid email request: ${e.message}")
            ResponseEntity.badRequest().body(
                ApiResponse.error(
                    code = "VALIDATION_ERROR",
                    message = e.message ?: "잘못된 요청입니다",
                    path = httpRequest.requestURI
                )
            )
        } catch (e: Exception) {
            logger.error("Failed to send email to ${request.to}", e)
            ResponseEntity.status(500).body(
                ApiResponse.error(
                    code = "EMAIL_SEND_ERROR",
                    message = "이메일 발송에 실패했습니다: ${e.message}",
                    path = httpRequest.requestURI
                )
            )
        }
    }

    @PostMapping("/test")
    @Operation(
        summary = "테스트 이메일 발송",
        description = "관리자용 테스트 이메일을 발송합니다. 개발/테스트 환경에서 이메일 설정을 확인할 때 사용합니다."
    )
    @ApiResponses(
        value = [
            SwaggerApiResponse(
                responseCode = "200",
                description = "테스트 이메일 발송 성공"
            ),
            SwaggerApiResponse(
                responseCode = "403",
                description = "권한 없음 (관리자 권한 필요)"
            )
        ]
    )
    @PreAuthorize("hasRole('ADMIN')")
    fun sendTestEmail(
        @Parameter(description = "테스트 이메일 주소", required = true, example = "admin@example.com")
        @RequestParam to: String,
        httpRequest: HttpServletRequest
    ): ResponseEntity<ApiResponse<EmailSendResponse>> {
        return try {
            val testHtmlContent = """
                <html>
                <body style="font-family: Arial, sans-serif; max-width: 600px; margin: 0 auto; padding: 20px;">
                    <div style="background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); padding: 20px; border-radius: 10px; color: white; text-align: center;">
                        <h1 style="margin: 0; font-size: 24px;">MOHE 테스트 이메일</h1>
                    </div>
                    
                    <div style="padding: 30px 0; text-align: center;">
                        <h2 style="color: #333;">이메일 발송 테스트</h2>
                        <p style="font-size: 16px; color: #666;">이 이메일은 MOHE 시스템의 이메일 발송 기능 테스트용입니다.</p>
                        <p style="font-size: 14px; color: #999;">발송 시간: ${java.time.LocalDateTime.now()}</p>
                    </div>
                    
                    <div style="border-top: 1px solid #eee; padding: 20px 0; text-align: center;">
                        <p style="font-size: 12px; color: #999; margin: 0;">MOHE 팀</p>
                    </div>
                </body>
                </html>
            """.trimIndent()

            emailService.sendHtmlEmail(
                to = to,
                subject = "[MOHE] 테스트 이메일",
                htmlContent = testHtmlContent
            )

            val response = EmailSendResponse(
                message = "테스트 이메일이 성공적으로 발송되었습니다",
                to = to,
                subject = "[MOHE] 테스트 이메일",
                type = "html"
            )

            logger.info("Test email sent successfully to: $to")
            ResponseEntity.ok(ApiResponse.success(response))

        } catch (e: Exception) {
            logger.error("Failed to send test email to $to", e)
            ResponseEntity.status(500).body(
                ApiResponse.error(
                    code = "EMAIL_SEND_ERROR",
                    message = "테스트 이메일 발송에 실패했습니다: ${e.message}",
                    path = httpRequest.requestURI
                )
            )
        }
    }
}

/**
 * Email send request
 */
data class EmailSendRequest(
    val to: String,
    val subject: String,
    val content: String,
    val type: String = "html", // "html" or "text"
    val from: String? = null
)

/**
 * Email send response
 */
data class EmailSendResponse(
    val message: String,
    val to: String,
    val subject: String,
    val type: String
)