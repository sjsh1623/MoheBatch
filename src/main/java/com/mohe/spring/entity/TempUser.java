package com.mohe.spring.entity;

import jakarta.persistence.*;
import java.time.OffsetDateTime;

@Entity
@Table(name = "temp_users")
public class TempUser {
    
    @Id
    private String id;
    
    @Column(nullable = false)
    private String email;
    
    @Column(name = "verification_code", nullable = false)
    private String verificationCode;
    
    private String nickname;
    
    @Column(name = "password_hash")
    private String passwordHash;
    
    @Column(name = "terms_agreed")
    private Boolean termsAgreed = false;
    
    @Column(name = "created_at")
    private OffsetDateTime createdAt = OffsetDateTime.now();
    
    @Column(name = "expires_at", nullable = false)
    private OffsetDateTime expiresAt;
    
    // Default constructor for JPA
    public TempUser() {}
    
    // Constructor with required fields
    public TempUser(String id, String email, String verificationCode, OffsetDateTime expiresAt) {
        this.id = id;
        this.email = email;
        this.verificationCode = verificationCode;
        this.expiresAt = expiresAt;
    }
    
    // Full constructor
    public TempUser(String id, String email, String verificationCode, String nickname, 
                   String passwordHash, Boolean termsAgreed, OffsetDateTime createdAt, 
                   OffsetDateTime expiresAt) {
        this.id = id;
        this.email = email;
        this.verificationCode = verificationCode;
        this.nickname = nickname;
        this.passwordHash = passwordHash;
        this.termsAgreed = termsAgreed;
        this.createdAt = createdAt;
        this.expiresAt = expiresAt;
    }
    
    // Getters and Setters
    public String getId() {
        return id;
    }
    
    public void setId(String id) {
        this.id = id;
    }
    
    public String getEmail() {
        return email;
    }
    
    public void setEmail(String email) {
        this.email = email;
    }
    
    public String getVerificationCode() {
        return verificationCode;
    }
    
    public void setVerificationCode(String verificationCode) {
        this.verificationCode = verificationCode;
    }
    
    public String getNickname() {
        return nickname;
    }
    
    public void setNickname(String nickname) {
        this.nickname = nickname;
    }
    
    public String getPasswordHash() {
        return passwordHash;
    }
    
    public void setPasswordHash(String passwordHash) {
        this.passwordHash = passwordHash;
    }
    
    public Boolean getTermsAgreed() {
        return termsAgreed;
    }
    
    public void setTermsAgreed(Boolean termsAgreed) {
        this.termsAgreed = termsAgreed;
    }
    
    public OffsetDateTime getCreatedAt() {
        return createdAt;
    }
    
    public void setCreatedAt(OffsetDateTime createdAt) {
        this.createdAt = createdAt;
    }
    
    public OffsetDateTime getExpiresAt() {
        return expiresAt;
    }
    
    public void setExpiresAt(OffsetDateTime expiresAt) {
        this.expiresAt = expiresAt;
    }
}