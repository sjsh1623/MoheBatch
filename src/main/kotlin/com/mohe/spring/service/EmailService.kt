package com.mohe.spring.service

import org.springframework.mail.SimpleMailMessage
import org.springframework.mail.javamail.JavaMailSender
import org.springframework.stereotype.Service

@Service
class EmailService(
    private val mailSender: JavaMailSender
) {
    
    fun sendVerificationEmail(email: String, verificationCode: String) {
        val message = SimpleMailMessage().apply {
            setTo(email)
            subject = "[MOHE] 이메일 인증 코드"
            text = """
                MOHE 회원가입을 위한 인증 코드입니다.
                
                인증 코드: $verificationCode
                
                이 코드는 10분 후에 만료됩니다.
                
                감사합니다.
                MOHE 팀
            """.trimIndent()
        }
        
        try {
            mailSender.send(message)
        } catch (e: Exception) {
            // In development, log the code instead of sending email
            println("Verification code for $email: $verificationCode")
        }
    }
    
    fun sendPasswordResetEmail(email: String, resetToken: String) {
        val message = SimpleMailMessage().apply {
            setTo(email)
            subject = "[MOHE] 비밀번호 재설정"
            text = """
                MOHE 비밀번호 재설정 요청입니다.
                
                아래 링크를 클릭하여 비밀번호를 재설정해주세요:
                https://mohe.app/reset-password?token=$resetToken
                
                이 링크는 1시간 후에 만료됩니다.
                
                감사합니다.
                MOHE 팀
            """.trimIndent()
        }
        
        try {
            mailSender.send(message)
        } catch (e: Exception) {
            // In development, log the token instead of sending email
            println("Password reset token for $email: $resetToken")
        }
    }
}