package com.mohe.spring.entity;

import jakarta.persistence.*;
import java.time.OffsetDateTime;

@Entity
@Table(name = "preferences")
public class Preference {
    
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id = 0L;
    
    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "user_id", nullable = false)
    private User user;
    
    @Column(name = "pref_key", nullable = false)
    private String prefKey;
    
    @Column(name = "pref_value")
    private String prefValue;
    
    @Column(name = "created_at")
    private OffsetDateTime createdAt = OffsetDateTime.now();
    
    // Default constructor for JPA
    public Preference() {}
    
    // Constructor with required fields
    public Preference(User user, String prefKey) {
        this.user = user;
        this.prefKey = prefKey;
    }
    
    // Full constructor
    public Preference(Long id, User user, String prefKey, String prefValue, OffsetDateTime createdAt) {
        this.id = id;
        this.user = user;
        this.prefKey = prefKey;
        this.prefValue = prefValue;
        this.createdAt = createdAt;
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
    
    public String getPrefKey() {
        return prefKey;
    }
    
    public void setPrefKey(String prefKey) {
        this.prefKey = prefKey;
    }
    
    public String getPrefValue() {
        return prefValue;
    }
    
    public void setPrefValue(String prefValue) {
        this.prefValue = prefValue;
    }
    
    public OffsetDateTime getCreatedAt() {
        return createdAt;
    }
    
    public void setCreatedAt(OffsetDateTime createdAt) {
        this.createdAt = createdAt;
    }
}