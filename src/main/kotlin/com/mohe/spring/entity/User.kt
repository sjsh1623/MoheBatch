package com.mohe.spring.entity

import jakarta.persistence.*
import java.time.OffsetDateTime

@Entity
@Table(name = "users")
class User(
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    val id: Long = 0,
    
    @Column(unique = true, nullable = false)
    val email: String,
    
    @Column(name = "password_hash", nullable = false)
    val passwordHash: String,
    
    @Column(unique = true)
    val nickname: String? = null,
    
    val mbti: String? = null,
    
    @Column(name = "age_range")
    val ageRange: String? = null,
    
    val transportation: String? = null,
    
    @Column(name = "profile_image_url")
    val profileImageUrl: String? = null,
    
    @Column(name = "is_onboarding_completed")
    val isOnboardingCompleted: Boolean = false,
    
    @Column(name = "last_login_at")
    val lastLoginAt: OffsetDateTime? = null,
    
    @Column(name = "created_at")
    val createdAt: OffsetDateTime = OffsetDateTime.now(),
    
    @Column(name = "updated_at")
    val updatedAt: OffsetDateTime = OffsetDateTime.now(),
    
    @OneToMany(mappedBy = "user", cascade = [CascadeType.ALL], fetch = FetchType.LAZY)
    val bookmarks: List<Bookmark> = emptyList(),
    
    @OneToMany(mappedBy = "user", cascade = [CascadeType.ALL], fetch = FetchType.LAZY)
    val prompts: List<Prompt> = emptyList(),
    
    @OneToMany(mappedBy = "user", cascade = [CascadeType.ALL], fetch = FetchType.LAZY)
    val preferences: List<Preference> = emptyList(),
    
    @OneToMany(mappedBy = "user", cascade = [CascadeType.ALL], fetch = FetchType.LAZY)
    val recentViews: List<RecentView> = emptyList(),
    
    @OneToMany(mappedBy = "user", cascade = [CascadeType.ALL], fetch = FetchType.LAZY)
    val termsAgreements: List<TermsAgreement> = emptyList(),
    
    @OneToMany(mappedBy = "user", cascade = [CascadeType.ALL], fetch = FetchType.LAZY)
    val emailVerifications: List<EmailVerification> = emptyList()
) {
    fun copy(
        email: String = this.email,
        passwordHash: String = this.passwordHash,
        nickname: String? = this.nickname,
        mbti: String? = this.mbti,
        ageRange: String? = this.ageRange,
        transportation: String? = this.transportation,
        profileImageUrl: String? = this.profileImageUrl,
        isOnboardingCompleted: Boolean = this.isOnboardingCompleted,
        lastLoginAt: OffsetDateTime? = this.lastLoginAt
    ): User {
        return User(
            id = this.id,
            email = email,
            passwordHash = passwordHash,
            nickname = nickname,
            mbti = mbti,
            ageRange = ageRange,
            transportation = transportation,
            profileImageUrl = profileImageUrl,
            isOnboardingCompleted = isOnboardingCompleted,
            lastLoginAt = lastLoginAt,
            createdAt = this.createdAt,
            updatedAt = OffsetDateTime.now()
        )
    }
}