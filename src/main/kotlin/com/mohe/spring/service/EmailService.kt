package com.mohe.spring.service

import org.springframework.mail.SimpleMailMessage
import org.springframework.mail.javamail.JavaMailSender
import org.springframework.mail.javamail.MimeMessageHelper
import org.springframework.stereotype.Service
import org.slf4j.LoggerFactory
import jakarta.mail.internet.MimeMessage
import org.jsoup.Jsoup
import org.jsoup.safety.Safelist

@Service
class EmailService(
    private val mailSender: JavaMailSender
) {
    private val logger = LoggerFactory.getLogger(EmailService::class.java)
    
    fun sendVerificationEmail(email: String, verificationCode: String) {
        val htmlContent = """
            <html>
            <body style="font-family: Arial, sans-serif; max-width: 600px; margin: 0 auto; padding: 20px;">
                <div style="background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); padding: 20px; border-radius: 10px; color: white; text-align: center;">
                    <h1 style="margin: 0; font-size: 24px;">MOHE 이메일 인증</h1>
                </div>
                
                <div style="padding: 30px 0; text-align: center;">
                    <p style="font-size: 16px; color: #333; margin-bottom: 30px;">회원가입을 위한 인증 코드입니다.</p>
                    
                    <div style="background: #f8f9fa; padding: 20px; border-radius: 8px; margin: 20px 0;">
                        <p style="margin: 0; font-size: 14px; color: #666;">인증 코드</p>
                        <h2 style="margin: 10px 0; font-size: 28px; color: #667eea; letter-spacing: 3px;">$verificationCode</h2>
                    </div>
                    
                    <p style="font-size: 14px; color: #666; margin-top: 30px;">이 코드는 <strong>10분</strong> 후에 만료됩니다.</p>
                </div>
                
                <div style="border-top: 1px solid #eee; padding: 20px 0; text-align: center;">
                    <p style="font-size: 12px; color: #999; margin: 0;">감사합니다.<br/>MOHE 팀</p>
                </div>
            </body>
            </html>
        """.trimIndent()
        
        try {
            sendHtmlEmail(email, "[MOHE] 이메일 인증 코드", htmlContent)
            logger.info("Verification email sent to $email")
        } catch (e: Exception) {
            logger.error("Failed to send verification email to $email", e)
            // In development, log the code instead of sending email
            println("Verification code for $email: $verificationCode")
        }
    }
    
    fun sendPasswordResetEmail(email: String, resetToken: String) {
        val resetUrl = "https://mohe.app/reset-password?token=$resetToken"
        val htmlContent = """
            <html>
            <body style="font-family: Arial, sans-serif; max-width: 600px; margin: 0 auto; padding: 20px;">
                <div style="background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); padding: 20px; border-radius: 10px; color: white; text-align: center;">
                    <h1 style="margin: 0; font-size: 24px;">MOHE 비밀번호 재설정</h1>
                </div>
                
                <div style="padding: 30px 0;">
                    <p style="font-size: 16px; color: #333; margin-bottom: 20px;">안녕하세요!</p>
                    <p style="font-size: 16px; color: #333; margin-bottom: 30px;">비밀번호 재설정 요청을 받았습니다. 아래 버튼을 클릭하여 새로운 비밀번호를 설정해주세요.</p>
                    
                    <div style="text-align: center; margin: 30px 0;">
                        <a href="$resetUrl" style="display: inline-block; background: #667eea; color: white; padding: 12px 30px; text-decoration: none; border-radius: 6px; font-size: 16px; font-weight: bold;">비밀번호 재설정</a>
                    </div>
                    
                    <p style="font-size: 14px; color: #666; margin-top: 30px;">위 버튼이 작동하지 않는다면 아래 링크를 복사하여 브라우저에 붙여넣어 주세요:</p>
                    <p style="font-size: 12px; color: #999; word-break: break-all; background: #f8f9fa; padding: 10px; border-radius: 4px;">$resetUrl</p>
                    
                    <p style="font-size: 14px; color: #666; margin-top: 20px;">이 링크는 <strong>1시간</strong> 후에 만료됩니다.</p>
                    <p style="font-size: 14px; color: #666;">만약 비밀번호 재설정을 요청하지 않았다면 이 이메일을 무시해주세요.</p>
                </div>
                
                <div style="border-top: 1px solid #eee; padding: 20px 0; text-align: center;">
                    <p style="font-size: 12px; color: #999; margin: 0;">감사합니다.<br/>MOHE 팀</p>
                </div>
            </body>
            </html>
        """.trimIndent()
        
        try {
            sendHtmlEmail(email, "[MOHE] 비밀번호 재설정", htmlContent)
            logger.info("Password reset email sent to $email")
        } catch (e: Exception) {
            logger.error("Failed to send password reset email to $email", e)
            // In development, log the token instead of sending email
            println("Password reset token for $email: $resetToken")
        }
    }
    
    /**
     * Send HTML email with content sanitization
     */
    fun sendHtmlEmail(to: String, subject: String, htmlContent: String, from: String? = null) {
        validateEmail(to)
        val sanitizedContent = sanitizeHtmlContent(htmlContent)
        
        val mimeMessage: MimeMessage = mailSender.createMimeMessage()
        val helper = MimeMessageHelper(mimeMessage, true, "UTF-8")
        
        helper.setTo(to)
        helper.setSubject(subject)
        helper.setText(sanitizedContent, true) // true = HTML content
        
        if (from != null) {
            helper.setFrom(from)
        }
        
        mailSender.send(mimeMessage)
        logger.info("HTML email sent to $to with subject: $subject")
    }
    
    /**
     * Send plain text email
     */
    fun sendTextEmail(to: String, subject: String, text: String, from: String? = null) {
        validateEmail(to)
        
        val message = SimpleMailMessage().apply {
            setTo(to)
            setSubject(subject)
            setText(text)
            if (from != null) {
                setFrom(from)
            }
        }
        
        mailSender.send(message)
        logger.info("Text email sent to $to with subject: $subject")
    }
    
    /**
     * Sanitize HTML content to prevent XSS attacks
     */
    private fun sanitizeHtmlContent(htmlContent: String): String {
        return Jsoup.clean(htmlContent, Safelist.relaxed()
            .addTags("style")
            .addAttributes(":all", "style")
            .addProtocols("a", "href", "http", "https", "mailto")
        )
    }
    
    /**
     * Basic email validation
     */
    private fun validateEmail(email: String) {
        if (!email.matches(Regex("^[A-Za-z0-9+_.-]+@([A-Za-z0-9.-]+\\.[A-Za-z]{2,})$"))) {
            throw IllegalArgumentException("Invalid email address: $email")
        }
    }
}