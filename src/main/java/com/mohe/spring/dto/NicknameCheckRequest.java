package com.mohe.spring.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Size;

public class NicknameCheckRequest {
    
    @JsonProperty("nickname")
    @NotBlank(message = "닉네임은 필수입니다")
    @Size(min = 2, max = 10, message = "닉네임은 2자 이상 10자 이하여야 합니다")
    @Pattern(regexp = "^[a-zA-Z0-9가-힣]*$", message = "닉네임은 한글, 영문, 숫자만 가능합니다")
    private String nickname;
    
    // Default constructor
    public NicknameCheckRequest() {}
    
    // Constructor with fields
    public NicknameCheckRequest(String nickname) {
        this.nickname = nickname;
    }
    
    // Getters and setters
    public String getNickname() {
        return nickname;
    }
    
    public void setNickname(String nickname) {
        this.nickname = nickname;
    }
}