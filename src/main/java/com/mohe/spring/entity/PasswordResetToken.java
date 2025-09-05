package com.mohe.spring.entity;

import jakarta.persistence.*;
import java.time.OffsetDateTime;

@Entity
@Table(name = "password_reset_tokens")
public class PasswordResetToken {
    
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id = 0L;
    
    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "user_id", nullable = false)
    private User user;
    
    @Column(unique = true, nullable = false)
    private String token;
    
    @Column(name = "expires_at", nullable = false)
    private OffsetDateTime expiresAt;
    
    @Column(name = "created_at")
    private OffsetDateTime createdAt = OffsetDateTime.now();
    
    private Boolean used = false;
    
    // Default constructor for JPA
    public PasswordResetToken() {}
    
    // Constructor with required fields
    public PasswordResetToken(User user, String token, OffsetDateTime expiresAt) {
        this.user = user;
        this.token = token;
        this.expiresAt = expiresAt;
    }
    
    // Full constructor
    public PasswordResetToken(Long id, User user, String token, OffsetDateTime expiresAt, 
                             OffsetDateTime createdAt, Boolean used) {
        this.id = id;
        this.user = user;
        this.token = token;
        this.expiresAt = expiresAt;
        this.createdAt = createdAt;
        this.used = used;
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
    
    public String getToken() {
        return token;
    }
    
    public void setToken(String token) {
        this.token = token;
    }
    
    public OffsetDateTime getExpiresAt() {
        return expiresAt;
    }
    
    public void setExpiresAt(OffsetDateTime expiresAt) {
        this.expiresAt = expiresAt;
    }
    
    public OffsetDateTime getCreatedAt() {
        return createdAt;
    }
    
    public void setCreatedAt(OffsetDateTime createdAt) {
        this.createdAt = createdAt;
    }
    
    public Boolean getUsed() {
        return used;
    }
    
    public void setUsed(Boolean used) {
        this.used = used;
    }
}