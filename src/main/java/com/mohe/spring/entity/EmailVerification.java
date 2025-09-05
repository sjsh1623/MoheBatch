package com.mohe.spring.entity;

import jakarta.persistence.*;
import java.time.OffsetDateTime;

@Entity
@Table(name = "email_verifications")
public class EmailVerification {
    
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id = 0L;
    
    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "user_id", nullable = false)
    private User user;
    
    @Column(nullable = false)
    private String email;
    
    @Column(nullable = false)
    private String code;
    
    @Column(name = "issued_at")
    private OffsetDateTime issuedAt = OffsetDateTime.now();
    
    @Column(name = "verified_at")
    private OffsetDateTime verifiedAt;
    
    @Column(nullable = false)
    private Boolean success = false;
    
    // Default constructor for JPA
    public EmailVerification() {}
    
    // Constructor with required fields
    public EmailVerification(User user, String email, String code) {
        this.user = user;
        this.email = email;
        this.code = code;
    }
    
    // Full constructor
    public EmailVerification(Long id, User user, String email, String code, 
                            OffsetDateTime issuedAt, OffsetDateTime verifiedAt, Boolean success) {
        this.id = id;
        this.user = user;
        this.email = email;
        this.code = code;
        this.issuedAt = issuedAt;
        this.verifiedAt = verifiedAt;
        this.success = success;
    }
    
    // Getters and Setters
    public Long getId() {
        return id;
    }
    
    public void setId(Long id) {
        this.id = id;
    }
    
    public User getUser() {
        return user;
    }
    
    public void setUser(User user) {
        this.user = user;
    }
    
    public String getEmail() {
        return email;
    }
    
    public void setEmail(String email) {
        this.email = email;
    }
    
    public String getCode() {
        return code;
    }
    
    public void setCode(String code) {
        this.code = code;
    }
    
    public OffsetDateTime getIssuedAt() {
        return issuedAt;
    }
    
    public void setIssuedAt(OffsetDateTime issuedAt) {
        this.issuedAt = issuedAt;
    }
    
    public OffsetDateTime getVerifiedAt() {
        return verifiedAt;
    }
    
    public void setVerifiedAt(OffsetDateTime verifiedAt) {
        this.verifiedAt = verifiedAt;
    }
    
    public Boolean getSuccess() {
        return success;
    }
    
    public void setSuccess(Boolean success) {
        this.success = success;
    }
}