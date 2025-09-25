package com.mohe.spring.dto;

import jakarta.validation.constraints.Size;

public class ProfileEditRequest {
    
    @Size(min = 2, max = 20, message = "닉네임은 2-20자 사이여야 합니다")
    private String nickname;
    
    private String profileImage; // base64 image data
    
    public ProfileEditRequest() {}
    
    public ProfileEditRequest(String nickname, String profileImage) {
        this.nickname = nickname;
        this.profileImage = profileImage;
    }
    
    public String getNickname() {
        return nickname;
    }
    
    public void setNickname(String nickname) {
        this.nickname = nickname;
    }
    
    public String getProfileImage() {
        return profileImage;
    }
    
    public void setProfileImage(String profileImage) {
        this.profileImage = profileImage;
    }
}