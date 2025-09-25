package com.mohe.spring.exception;

import com.mohe.spring.dto.ApiResponse;
import com.mohe.spring.dto.ErrorCode;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.security.authentication.BadCredentialsException;
import org.springframework.validation.FieldError;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.util.HashMap;
import java.util.Map;

@RestControllerAdvice
public class GlobalExceptionHandler {
    
    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ResponseEntity<ApiResponse<Void>> handleValidationException(
            MethodArgumentNotValidException ex,
            HttpServletRequest request) {
        Map<String, String> errors = new HashMap<>();
        ex.getBindingResult().getAllErrors().forEach(error -> {
            String fieldName = ((FieldError) error).getField();
            String errorMessage = error.getDefaultMessage();
            if (errorMessage == null) {
                errorMessage = "유효하지 않은 값입니다";
            }
            errors.put(fieldName, errorMessage);
        });
        
        return ResponseEntity.badRequest().body(
            ApiResponse.error(
                ErrorCode.VALIDATION_ERROR,
                "입력값이 유효하지 않습니다",
                request.getRequestURI(),
                errors
            )
        );
    }
    
    @ExceptionHandler(BadCredentialsException.class)
    public ResponseEntity<ApiResponse<Void>> handleBadCredentialsException(
            BadCredentialsException ex,
            HttpServletRequest request) {
        return ResponseEntity.status(HttpStatus.UNAUTHORIZED).body(
            ApiResponse.error(
                ErrorCode.INVALID_CREDENTIALS,
                "이메일 또는 비밀번호가 잘못되었습니다",
                request.getRequestURI()
            )
        );
    }
    
    @ExceptionHandler(AccessDeniedException.class)
    public ResponseEntity<ApiResponse<Void>> handleAccessDeniedException(
            AccessDeniedException ex,
            HttpServletRequest request) {
        return ResponseEntity.status(HttpStatus.FORBIDDEN).body(
            ApiResponse.error(
                ErrorCode.ACCESS_DENIED,
                "접근 권한이 없습니다",
                request.getRequestURI()
            )
        );
    }
    
    @ExceptionHandler(IllegalArgumentException.class)
    public ResponseEntity<ApiResponse<Void>> handleIllegalArgumentException(
            IllegalArgumentException ex,
            HttpServletRequest request) {
        return ResponseEntity.badRequest().body(
            ApiResponse.error(
                ErrorCode.VALIDATION_ERROR,
                ex.getMessage() != null ? ex.getMessage() : "잘못된 요청입니다",
                request.getRequestURI()
            )
        );
    }
    
    @ExceptionHandler(RuntimeException.class)
    public ResponseEntity<ApiResponse<Void>> handleRuntimeException(
            RuntimeException ex,
            HttpServletRequest request) {
        String errorCode;
        String message = ex.getMessage();
        
        if (message != null) {
            if (message.contains("찾을 수 없습니다")) {
                errorCode = ErrorCode.RESOURCE_NOT_FOUND;
            } else if (message.contains("이미 사용")) {
                errorCode = ErrorCode.DUPLICATE_EMAIL;
            } else if (message.contains("닉네임")) {
                errorCode = ErrorCode.DUPLICATE_NICKNAME;
            } else if (message.contains("토큰")) {
                errorCode = ErrorCode.INVALID_TOKEN;
            } else {
                errorCode = ErrorCode.INTERNAL_SERVER_ERROR;
            }
        } else {
            errorCode = ErrorCode.INTERNAL_SERVER_ERROR;
        }
        
        HttpStatus status = switch (errorCode) {
            case ErrorCode.RESOURCE_NOT_FOUND -> HttpStatus.NOT_FOUND;
            case ErrorCode.INVALID_TOKEN -> HttpStatus.UNAUTHORIZED;
            default -> HttpStatus.BAD_REQUEST;
        };
        
        return ResponseEntity.status(status).body(
            ApiResponse.error(
                errorCode,
                message != null ? message : "요청 처리 중 오류가 발생했습니다",
                request.getRequestURI()
            )
        );
    }
    
    @ExceptionHandler(Exception.class)
    public ResponseEntity<ApiResponse<Void>> handleGenericException(
            Exception ex,
            HttpServletRequest request) {
        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(
            ApiResponse.error(
                ErrorCode.INTERNAL_SERVER_ERROR,
                "서버 내부 오류가 발생했습니다",
                request.getRequestURI()
            )
        );
    }
}