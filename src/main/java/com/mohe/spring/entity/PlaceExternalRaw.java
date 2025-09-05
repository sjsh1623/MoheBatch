package com.mohe.spring.entity;

import com.fasterxml.jackson.databind.JsonNode;
import com.vladmihalcea.hibernate.type.json.JsonType;
import jakarta.persistence.*;
import org.hibernate.annotations.Type;
import java.time.LocalDateTime;
import java.util.Arrays;

@Entity
@Table(name = "place_external_raw",
       indexes = {
           @Index(name = "idx_place_external_raw_place_id", columnList = "place_id"),
           @Index(name = "idx_place_external_raw_source", columnList = "source"),
           @Index(name = "idx_place_external_raw_fetched_at", columnList = "fetched_at")
       },
       uniqueConstraints = @UniqueConstraint(columnNames = {"source", "external_id"}))
public class PlaceExternalRaw {
    
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;
    
    @Column(nullable = false, length = 50)
    private String source; // 'naver', 'google', etc.
    
    @Column(name = "external_id", nullable = false, length = 255)
    private String externalId;
    
    @Column(name = "place_id")
    private Long placeId; // FK to places table (nullable for failed processing)
    
    @Type(JsonType.class)
    @Column(nullable = false, columnDefinition = "jsonb")
    private JsonNode payload;
    
    @Column(name = "fetched_at")
    private LocalDateTime fetchedAt = LocalDateTime.now();
    
    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "place_id", insertable = false, updatable = false)
    private Place place;
    
    // Default constructor for JPA
    public PlaceExternalRaw() {}
    
    // Constructor with required fields
    public PlaceExternalRaw(String source, String externalId, JsonNode payload) {
        this.source = source;
        this.externalId = externalId;
        this.payload = payload;
    }
    
    // Getters and Setters
    public Long getId() {
        return id;
    }
    
    public void setId(Long id) {
        this.id = id;
    }
    
    public String getSource() {
        return source;
    }
    
    public void setSource(String source) {
        this.source = source;
    }
    
    public String getExternalId() {
        return externalId;
    }
    
    public void setExternalId(String externalId) {
        this.externalId = externalId;
    }
    
    public Long getPlaceId() {
        return placeId;
    }
    
    public void setPlaceId(Long placeId) {
        this.placeId = placeId;
    }
    
    public JsonNode getPayload() {
        return payload;
    }
    
    public void setPayload(JsonNode payload) {
        this.payload = payload;
    }
    
    public LocalDateTime getFetchedAt() {
        return fetchedAt;
    }
    
    public void setFetchedAt(LocalDateTime fetchedAt) {
        this.fetchedAt = fetchedAt;
    }
    
    public Place getPlace() {
        return place;
    }
    
    public void setPlace(Place place) {
        this.place = place;
    }
}

enum ExternalDataSource {
    NAVER("naver"),
    GOOGLE("google"),
    FOURSQUARE("foursquare"),
    KAKAO("kakao");
    
    private final String sourceName;
    
    ExternalDataSource(String sourceName) {
        this.sourceName = sourceName;
    }
    
    public String getSourceName() {
        return sourceName;
    }
    
    public static ExternalDataSource fromString(String source) {
        return Arrays.stream(values())
                .filter(dataSource -> dataSource.sourceName.equalsIgnoreCase(source))
                .findFirst()
                .orElse(null);
    }
}