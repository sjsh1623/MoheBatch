package com.mohe.spring.dto

import com.fasterxml.jackson.annotation.JsonInclude
import java.time.OffsetDateTime

@JsonInclude(JsonInclude.Include.NON_NULL)
data class ApiResponse<out T>(
    val success: Boolean,
    val data: T? = null,
    val message: String? = null,
    val error: ErrorDetail? = null,
    val timestamp: OffsetDateTime = OffsetDateTime.now(),
    val path: String? = null
) {
    companion object {
        fun <T> success(data: T, message: String? = null): ApiResponse<T> {
            return ApiResponse(success = true, data = data, message = message)
        }
        
        fun success(message: String): ApiResponse<Nothing> {
            return ApiResponse(success = true, message = message)
        }
        
        fun error(code: String, message: String, path: String? = null, details: Any? = null): ApiResponse<Nothing> {
            return ApiResponse(
                success = false,
                error = ErrorDetail(code, message, details),
                path = path
            )
        }
    }
}

data class ErrorDetail(
    val code: String,
    val message: String,
    val details: Any? = null
)

object ErrorCode {
    const val INVALID_CREDENTIALS = "INVALID_CREDENTIALS"
    const val TOKEN_EXPIRED = "TOKEN_EXPIRED"
    const val INVALID_TOKEN = "INVALID_TOKEN"
    const val ACCESS_DENIED = "ACCESS_DENIED"
    const val VALIDATION_ERROR = "VALIDATION_ERROR"
    const val DUPLICATE_EMAIL = "DUPLICATE_EMAIL"
    const val DUPLICATE_NICKNAME = "DUPLICATE_NICKNAME"
    const val RESOURCE_NOT_FOUND = "RESOURCE_NOT_FOUND"
    const val INTERNAL_SERVER_ERROR = "INTERNAL_SERVER_ERROR"
    const val INVALID_VERIFICATION_CODE = "INVALID_VERIFICATION_CODE"
    const val VERIFICATION_CODE_EXPIRED = "VERIFICATION_CODE_EXPIRED"
}