package com.mohe.spring.entity;

import jakarta.persistence.*;
import java.time.LocalDateTime;
import java.util.Arrays;

@Entity
@Table(name = "place_mbti_descriptions",
       uniqueConstraints = @UniqueConstraint(columnNames = {"place_id", "mbti"}))
public class PlaceMbtiDescription {
    
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;
    
    @Column(name = "place_id", nullable = false)
    private Long placeId;
    
    @Column(nullable = false, length = 4)
    private String mbti;
    
    @Column(nullable = false, columnDefinition = "TEXT")
    private String description;
    
    @Column(length = 100)
    private String model = "llama3.1:latest";
    
    @Column(name = "prompt_hash", nullable = false, length = 64)
    private String promptHash;
    
    @Column(name = "updated_at")
    private LocalDateTime updatedAt = LocalDateTime.now();
    
    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "place_id", insertable = false, updatable = false)
    private Place place;
    
    // Default constructor for JPA
    public PlaceMbtiDescription() {}
    
    // Constructor with required fields
    public PlaceMbtiDescription(Long placeId, String mbti, String description, String promptHash) {
        this.placeId = placeId;
        this.mbti = mbti;
        this.description = description;
        this.promptHash = promptHash;
    }
    
    // Getters and Setters
    public Long getId() {
        return id;
    }
    
    public void setId(Long id) {
        this.id = id;
    }
    
    public Long getPlaceId() {
        return placeId;
    }
    
    public void setPlaceId(Long placeId) {
        this.placeId = placeId;
    }
    
    public String getMbti() {
        return mbti;
    }
    
    public void setMbti(String mbti) {
        this.mbti = mbti;
    }
    
    public String getDescription() {
        return description;
    }
    
    public void setDescription(String description) {
        this.description = description;
    }
    
    public String getModel() {
        return model;
    }
    
    public void setModel(String model) {
        this.model = model;
    }
    
    public String getPromptHash() {
        return promptHash;
    }
    
    public void setPromptHash(String promptHash) {
        this.promptHash = promptHash;
    }
    
    public LocalDateTime getUpdatedAt() {
        return updatedAt;
    }
    
    public void setUpdatedAt(LocalDateTime updatedAt) {
        this.updatedAt = updatedAt;
    }
    
    public Place getPlace() {
        return place;
    }
    
    public void setPlace(Place place) {
        this.place = place;
    }
}

enum MbtiType {
    INTJ("INTJ"), INTP("INTP"), ENTJ("ENTJ"), ENTP("ENTP"),
    INFJ("INFJ"), INFP("INFP"), ENFJ("ENFJ"), ENFP("ENFP"),
    ISTJ("ISTJ"), ISFJ("ISFJ"), ESTJ("ESTJ"), ESFJ("ESFJ"),
    ISTP("ISTP"), ISFP("ISFP"), ESTP("ESTP"), ESFP("ESFP");
    
    private final String code;
    
    MbtiType(String code) {
        this.code = code;
    }
    
    public String getCode() {
        return code;
    }
    
    public static MbtiType fromString(String code) {
        return Arrays.stream(values())
                .filter(type -> type.code.equalsIgnoreCase(code))
                .findFirst()
                .orElse(null);
    }
    
    public static String[] getAllCodes() {
        return Arrays.stream(values())
                .map(MbtiType::getCode)
                .toArray(String[]::new);
    }
}