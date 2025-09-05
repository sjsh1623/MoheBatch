package com.mohe.spring.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import java.time.OffsetDateTime;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class ApiResponse<T> {
    private final boolean success;
    private final T data;
    private final String message;
    private final ErrorDetail error;
    private final OffsetDateTime timestamp;
    private final String path;

    public ApiResponse(boolean success, T data, String message, ErrorDetail error, OffsetDateTime timestamp, String path) {
        this.success = success;
        this.data = data;
        this.message = message;
        this.error = error;
        this.timestamp = timestamp != null ? timestamp : OffsetDateTime.now();
        this.path = path;
    }

    // Factory methods for success
    public static <T> ApiResponse<T> success(T data, String message) {
        return new ApiResponse<>(true, data, message, null, OffsetDateTime.now(), null);
    }

    public static <T> ApiResponse<T> success(T data) {
        return success(data, null);
    }
    
    public static ApiResponse<Void> success(String message) {
        return new ApiResponse<>(true, null, message, null, OffsetDateTime.now(), null);
    }
    
    // Factory methods for error
    public static <T> ApiResponse<T> error(String code, String message, String path, Object details) {
        return new ApiResponse<>(false, null, null, new ErrorDetail(code, message, details), OffsetDateTime.now(), path);
    }

    public static <T> ApiResponse<T> error(String code, String message, String path) {
        return error(code, message, path, null);
    }

    public static <T> ApiResponse<T> error(String code, String message) {
        return error(code, message, null, null);
    }

    // Getters
    public boolean isSuccess() {
        return success;
    }

    public T getData() {
        return data;
    }

    public String getMessage() {
        return message;
    }

    public ErrorDetail getError() {
        return error;
    }

    public OffsetDateTime getTimestamp() {
        return timestamp;
    }

    public String getPath() {
        return path;
    }
}