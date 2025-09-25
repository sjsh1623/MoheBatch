package com.mohe.spring.dto;

public record ErrorDetail(
    String code,
    String message,
    Object details
) {
    public ErrorDetail(String code, String message) {
        this(code, message, null);
    }
}