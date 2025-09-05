package com.mohe.spring.entity;

import jakarta.persistence.*;
import java.time.OffsetDateTime;

@Entity
@Table(name = "terms_agreements")
public class TermsAgreement {
    
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id = 0L;
    
    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "user_id", nullable = false)
    private User user;
    
    @Column(name = "terms_code", nullable = false)
    private String termsCode;
    
    @Column(nullable = false)
    private Boolean agreed = false;
    
    @Column(name = "agreed_at")
    private OffsetDateTime agreedAt = OffsetDateTime.now();
    
    // Default constructor for JPA
    public TermsAgreement() {}
    
    // Constructor with required fields
    public TermsAgreement(User user, String termsCode) {
        this.user = user;
        this.termsCode = termsCode;
    }
    
    // Full constructor
    public TermsAgreement(Long id, User user, String termsCode, Boolean agreed, OffsetDateTime agreedAt) {
        this.id = id;
        this.user = user;
        this.termsCode = termsCode;
        this.agreed = agreed;
        this.agreedAt = agreedAt;
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
    
    public String getTermsCode() {
        return termsCode;
    }
    
    public void setTermsCode(String termsCode) {
        this.termsCode = termsCode;
    }
    
    public Boolean getAgreed() {
        return agreed;
    }
    
    public void setAgreed(Boolean agreed) {
        this.agreed = agreed;
    }
    
    public OffsetDateTime getAgreedAt() {
        return agreedAt;
    }
    
    public void setAgreedAt(OffsetDateTime agreedAt) {
        this.agreedAt = agreedAt;
    }
}