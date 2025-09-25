package com.mohe.spring.entity;

import jakarta.persistence.*;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;

@Entity
@Table(name = "users")
public class User {
    
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id = 0L;
    
    @Column(unique = true, nullable = false)
    private String email;
    
    @Column(name = "password_hash", nullable = false)
    private String passwordHash;
    
    @Column(unique = true)
    private String nickname;
    
    private String mbti;
    
    @Column(name = "age_range")
    private String ageRange;
    
    private String transportation;
    
    @Column(name = "profile_image_url")
    private String profileImageUrl;
    
    @Column(name = "is_onboarding_completed")
    private Boolean isOnboardingCompleted = false;
    
    @Column(name = "last_login_at")
    private OffsetDateTime lastLoginAt;
    
    @Column(name = "created_at")
    private OffsetDateTime createdAt = OffsetDateTime.now();
    
    @Column(name = "updated_at")
    private OffsetDateTime updatedAt = OffsetDateTime.now();
    
    @OneToMany(mappedBy = "user", cascade = CascadeType.ALL, fetch = FetchType.LAZY)
    private List<Bookmark> bookmarks = new ArrayList<>();
    
    @OneToMany(mappedBy = "user", cascade = CascadeType.ALL, fetch = FetchType.LAZY)
    private List<Prompt> prompts = new ArrayList<>();
    
    @OneToMany(mappedBy = "user", cascade = CascadeType.ALL, fetch = FetchType.LAZY)
    private List<Preference> preferences = new ArrayList<>();
    
    @OneToMany(mappedBy = "user", cascade = CascadeType.ALL, fetch = FetchType.LAZY)
    private List<RecentView> recentViews = new ArrayList<>();
    
    @OneToMany(mappedBy = "user", cascade = CascadeType.ALL, fetch = FetchType.LAZY)
    private List<TermsAgreement> termsAgreements = new ArrayList<>();
    
    @OneToMany(mappedBy = "user", cascade = CascadeType.ALL, fetch = FetchType.LAZY)
    private List<EmailVerification> emailVerifications = new ArrayList<>();
    
    // Default constructor for JPA
    public User() {}
    
    // Constructor with required fields
    public User(String email, String passwordHash) {
        this.email = email;
        this.passwordHash = passwordHash;
    }
    
    // Full constructor
    public User(Long id, String email, String passwordHash, String nickname, String mbti, 
               String ageRange, String transportation, String profileImageUrl, 
               Boolean isOnboardingCompleted, OffsetDateTime lastLoginAt,
               OffsetDateTime createdAt, OffsetDateTime updatedAt) {
        this.id = id;
        this.email = email;
        this.passwordHash = passwordHash;
        this.nickname = nickname;
        this.mbti = mbti;
        this.ageRange = ageRange;
        this.transportation = transportation;
        this.profileImageUrl = profileImageUrl;
        this.isOnboardingCompleted = isOnboardingCompleted;
        this.lastLoginAt = lastLoginAt;
        this.createdAt = createdAt;
        this.updatedAt = updatedAt;
    }
    
    // Copy method equivalent to Kotlin's copy function
    public User copy(String email, String passwordHash, String nickname, String mbti,
                    String ageRange, String transportation, String profileImageUrl,
                    Boolean isOnboardingCompleted, OffsetDateTime lastLoginAt) {
        return new User(
            this.id,
            email != null ? email : this.email,
            passwordHash != null ? passwordHash : this.passwordHash,
            nickname,
            mbti,
            ageRange,
            transportation,
            profileImageUrl,
            isOnboardingCompleted != null ? isOnboardingCompleted : this.isOnboardingCompleted,
            lastLoginAt,
            this.createdAt,
            OffsetDateTime.now()
        );
    }
    
    // Getters and Setters
    public Long getId() {
        return id;
    }
    
    public void setId(Long id) {
        this.id = id;
    }
    
    public String getEmail() {
        return email;
    }
    
    public void setEmail(String email) {
        this.email = email;
    }
    
    public String getPasswordHash() {
        return passwordHash;
    }
    
    public void setPasswordHash(String passwordHash) {
        this.passwordHash = passwordHash;
    }
    
    public String getNickname() {
        return nickname;
    }
    
    public void setNickname(String nickname) {
        this.nickname = nickname;
    }
    
    public String getMbti() {
        return mbti;
    }
    
    public void setMbti(String mbti) {
        this.mbti = mbti;
    }
    
    public String getAgeRange() {
        return ageRange;
    }
    
    public void setAgeRange(String ageRange) {
        this.ageRange = ageRange;
    }
    
    public String getTransportation() {
        return transportation;
    }
    
    public void setTransportation(String transportation) {
        this.transportation = transportation;
    }
    
    public String getProfileImageUrl() {
        return profileImageUrl;
    }
    
    public void setProfileImageUrl(String profileImageUrl) {
        this.profileImageUrl = profileImageUrl;
    }
    
    public Boolean getIsOnboardingCompleted() {
        return isOnboardingCompleted;
    }
    
    public void setIsOnboardingCompleted(Boolean isOnboardingCompleted) {
        this.isOnboardingCompleted = isOnboardingCompleted;
    }
    
    public OffsetDateTime getLastLoginAt() {
        return lastLoginAt;
    }
    
    public void setLastLoginAt(OffsetDateTime lastLoginAt) {
        this.lastLoginAt = lastLoginAt;
    }
    
    public OffsetDateTime getCreatedAt() {
        return createdAt;
    }
    
    public void setCreatedAt(OffsetDateTime createdAt) {
        this.createdAt = createdAt;
    }
    
    public OffsetDateTime getUpdatedAt() {
        return updatedAt;
    }
    
    public void setUpdatedAt(OffsetDateTime updatedAt) {
        this.updatedAt = updatedAt;
    }
    
    public List<Bookmark> getBookmarks() {
        return bookmarks;
    }
    
    public void setBookmarks(List<Bookmark> bookmarks) {
        this.bookmarks = bookmarks;
    }
    
    public List<Prompt> getPrompts() {
        return prompts;
    }
    
    public void setPrompts(List<Prompt> prompts) {
        this.prompts = prompts;
    }
    
    public List<Preference> getPreferences() {
        return preferences;
    }
    
    public void setPreferences(List<Preference> preferences) {
        this.preferences = preferences;
    }
    
    public List<RecentView> getRecentViews() {
        return recentViews;
    }
    
    public void setRecentViews(List<RecentView> recentViews) {
        this.recentViews = recentViews;
    }
    
    public List<TermsAgreement> getTermsAgreements() {
        return termsAgreements;
    }
    
    public void setTermsAgreements(List<TermsAgreement> termsAgreements) {
        this.termsAgreements = termsAgreements;
    }
    
    public List<EmailVerification> getEmailVerifications() {
        return emailVerifications;
    }
    
    public void setEmailVerifications(List<EmailVerification> emailVerifications) {
        this.emailVerifications = emailVerifications;
    }
}