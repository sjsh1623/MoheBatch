package com.mohe.batch.entity;

import lombok.Getter;
import lombok.Setter;

import jakarta.persistence.*;
import java.time.LocalTime;

@Entity
@Table(name = "place_business_hours")
@Getter
@Setter
public class PlaceBusinessHour {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "place_id")
    private Place place;

    @Column(length = 10)
    private String dayOfWeek;

    private LocalTime open;

    private LocalTime close;

    @Column(columnDefinition = "TEXT")
    private String description;

    private Boolean isOperating;

    private Integer lastOrderMinutes;
}
