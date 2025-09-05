package com.mohe.spring.entity;

import jakarta.persistence.*;
import java.time.OffsetDateTime;

@Entity
@Table(name = "recent_views")
public class RecentView {
    
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id = 0L;
    
    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "user_id", nullable = false)
    private User user;
    
    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "place_id", nullable = false)
    private Place place;
    
    @Column(name = "viewed_at")
    private OffsetDateTime viewedAt = OffsetDateTime.now();
    
    // Default constructor for JPA
    public RecentView() {}
    
    // Constructor with required fields
    public RecentView(User user, Place place) {
        this.user = user;
        this.place = place;
    }
    
    // Full constructor
    public RecentView(Long id, User user, Place place, OffsetDateTime viewedAt) {
        this.id = id;
        this.user = user;
        this.place = place;
        this.viewedAt = viewedAt;
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
    
    public OffsetDateTime getViewedAt() {
        return viewedAt;
    }
    
    public void setViewedAt(OffsetDateTime viewedAt) {
        this.viewedAt = viewedAt;
    }
}