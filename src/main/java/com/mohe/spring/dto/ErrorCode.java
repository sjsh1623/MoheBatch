package com.mohe.spring.dto;

public final class ErrorCode {
    public static final String INVALID_CREDENTIALS = "INVALID_CREDENTIALS";
    public static final String TOKEN_EXPIRED = "TOKEN_EXPIRED";
    public static final String INVALID_TOKEN = "INVALID_TOKEN";
    public static final String ACCESS_DENIED = "ACCESS_DENIED";
    public static final String VALIDATION_ERROR = "VALIDATION_ERROR";
    public static final String DUPLICATE_EMAIL = "DUPLICATE_EMAIL";
    public static final String DUPLICATE_NICKNAME = "DUPLICATE_NICKNAME";
    public static final String RESOURCE_NOT_FOUND = "RESOURCE_NOT_FOUND";
    public static final String INTERNAL_SERVER_ERROR = "INTERNAL_SERVER_ERROR";
    public static final String INVALID_VERIFICATION_CODE = "INVALID_VERIFICATION_CODE";
    public static final String VERIFICATION_CODE_EXPIRED = "VERIFICATION_CODE_EXPIRED";

    private ErrorCode() {
        // Prevent instantiation
    }
}