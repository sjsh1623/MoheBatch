package com.mohe.spring.dto

import jakarta.validation.constraints.NotBlank
import jakarta.validation.constraints.Size
import java.time.OffsetDateTime

// User Preferences
data class UserPreferencesRequest(
    val mbti: MbtiPreference?,
    val ageRange: String?,
    val spacePreferences: List<String> = emptyList(),
    val transportationMethod: String?
)

data class MbtiPreference(
    val extroversion: String, // E or I
    val sensing: String,      // S or N
    val thinking: String,     // T or F
    val judging: String       // J or P
)

data class UserPreferencesResponse(
    val preferences: UserPreferencesData
)

data class UserPreferencesData(
    val mbti: String?,
    val ageRange: String?,
    val spacePreferences: List<String>,
    val transportationMethod: String?
)

// User Profile
data class UserProfileResponse(
    val user: UserProfileData
)

data class UserProfileData(
    val id: String,
    val email: String,
    val nickname: String?,
    val mbti: String?,
    val ageRange: String?,
    val spacePreferences: List<String> = emptyList(),
    val transportationMethod: String?,
    val profileImage: String?,
    val createdAt: OffsetDateTime
)

// Profile Edit
data class ProfileEditRequest(
    @field:Size(min = 2, max = 20, message = "닉네임은 2-20자 사이여야 합니다")
    val nickname: String?,
    
    val profileImage: String? // base64 image data
)

data class ProfileEditResponse(
    val user: ProfileEditData
)

data class ProfileEditData(
    val id: String,
    val nickname: String?,
    val profileImage: String?
)