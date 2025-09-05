package com.mohe.spring.entity;

import jakarta.persistence.*;
import java.time.OffsetDateTime;

@Entity
@Table(name = "prompts")
public class Prompt {
    
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id = 0L;
    
    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "user_id", nullable = false)
    private User user;
    
    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "place_id", nullable = false)
    private Place place;
    
    @Column(nullable = false)
    private String content;
    
    @Column(name = "created_at")
    private OffsetDateTime createdAt = OffsetDateTime.now();
    
    // Default constructor for JPA
    public Prompt() {}
    
    // Constructor with required fields
    public Prompt(User user, Place place, String content) {
        this.user = user;
        this.place = place;
        this.content = content;
    }
    
    // Full constructor
    public Prompt(Long id, User user, Place place, String content, OffsetDateTime createdAt) {
        this.id = id;
        this.user = user;
        this.place = place;
        this.content = content;
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
    
    public Place getPlace() {
        return place;
    }
    
    public void setPlace(Place place) {
        this.place = place;
    }
    
    public String getContent() {
        return content;
    }
    
    public void setContent(String content) {
        this.content = content;
    }
    
    public OffsetDateTime getCreatedAt() {
        return createdAt;
    }
    
    public void setCreatedAt(OffsetDateTime createdAt) {
        this.createdAt = createdAt;
    }
}