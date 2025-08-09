package com.mohe.spring.exception

import com.mohe.spring.dto.ApiResponse
import com.mohe.spring.dto.ErrorCode
import jakarta.servlet.http.HttpServletRequest
import org.springframework.http.HttpStatus
import org.springframework.http.ResponseEntity
import org.springframework.security.access.AccessDeniedException
import org.springframework.security.authentication.BadCredentialsException
import org.springframework.validation.FieldError
import org.springframework.web.bind.MethodArgumentNotValidException
import org.springframework.web.bind.annotation.ExceptionHandler
import org.springframework.web.bind.annotation.RestControllerAdvice

@RestControllerAdvice
class GlobalExceptionHandler {
    
    @ExceptionHandler(MethodArgumentNotValidException::class)
    fun handleValidationException(
        ex: MethodArgumentNotValidException,
        request: HttpServletRequest
    ): ResponseEntity<ApiResponse<Nothing>> {
        val errors = mutableMapOf<String, String>()
        ex.bindingResult.allErrors.forEach { error ->
            val fieldName = (error as FieldError).field
            val errorMessage = error.defaultMessage ?: "유효하지 않은 값입니다"
            errors[fieldName] = errorMessage
        }
        
        return ResponseEntity.badRequest().body(
            ApiResponse.error(
                code = ErrorCode.VALIDATION_ERROR,
                message = "입력값이 유효하지 않습니다",
                path = request.requestURI,
                details = errors
            )
        )
    }
    
    @ExceptionHandler(BadCredentialsException::class)
    fun handleBadCredentialsException(
        ex: BadCredentialsException,
        request: HttpServletRequest
    ): ResponseEntity<ApiResponse<Nothing>> {
        return ResponseEntity.status(HttpStatus.UNAUTHORIZED).body(
            ApiResponse.error(
                code = ErrorCode.INVALID_CREDENTIALS,
                message = "이메일 또는 비밀번호가 잘못되었습니다",
                path = request.requestURI
            )
        )
    }
    
    @ExceptionHandler(AccessDeniedException::class)
    fun handleAccessDeniedException(
        ex: AccessDeniedException,
        request: HttpServletRequest
    ): ResponseEntity<ApiResponse<Nothing>> {
        return ResponseEntity.status(HttpStatus.FORBIDDEN).body(
            ApiResponse.error(
                code = ErrorCode.ACCESS_DENIED,
                message = "접근 권한이 없습니다",
                path = request.requestURI
            )
        )
    }
    
    @ExceptionHandler(IllegalArgumentException::class)
    fun handleIllegalArgumentException(
        ex: IllegalArgumentException,
        request: HttpServletRequest
    ): ResponseEntity<ApiResponse<Nothing>> {
        return ResponseEntity.badRequest().body(
            ApiResponse.error(
                code = ErrorCode.VALIDATION_ERROR,
                message = ex.message ?: "잘못된 요청입니다",
                path = request.requestURI
            )
        )
    }
    
    @ExceptionHandler(RuntimeException::class)
    fun handleRuntimeException(
        ex: RuntimeException,
        request: HttpServletRequest
    ): ResponseEntity<ApiResponse<Nothing>> {
        val errorCode = when {
            ex.message?.contains("찾을 수 없습니다") == true -> ErrorCode.RESOURCE_NOT_FOUND
            ex.message?.contains("이미 사용") == true -> ErrorCode.DUPLICATE_EMAIL
            ex.message?.contains("닉네임") == true -> ErrorCode.DUPLICATE_NICKNAME
            ex.message?.contains("토큰") == true -> ErrorCode.INVALID_TOKEN
            else -> ErrorCode.INTERNAL_SERVER_ERROR
        }
        
        val status = when (errorCode) {
            ErrorCode.RESOURCE_NOT_FOUND -> HttpStatus.NOT_FOUND
            ErrorCode.INVALID_TOKEN -> HttpStatus.UNAUTHORIZED
            else -> HttpStatus.BAD_REQUEST
        }
        
        return ResponseEntity.status(status).body(
            ApiResponse.error(
                code = errorCode,
                message = ex.message ?: "요청 처리 중 오류가 발생했습니다",
                path = request.requestURI
            )
        )
    }
    
    @ExceptionHandler(Exception::class)
    fun handleGenericException(
        ex: Exception,
        request: HttpServletRequest
    ): ResponseEntity<ApiResponse<Nothing>> {
        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(
            ApiResponse.error(
                code = ErrorCode.INTERNAL_SERVER_ERROR,
                message = "서버 내부 오류가 발생했습니다",
                path = request.requestURI
            )
        )
    }
}