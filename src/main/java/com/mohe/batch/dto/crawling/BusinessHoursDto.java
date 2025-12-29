package com.mohe.batch.dto.crawling;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;
import lombok.Setter;

import java.util.Map;

@Getter
@Setter
@JsonIgnoreProperties(ignoreUnknown = true)
public class BusinessHoursDto {
    @JsonProperty("today_status")
    private String todayStatus;

    private String description;

    @JsonProperty("last_order_minutes")
    private Integer lastOrderMinutes;

    private Map<String, WeeklyHoursDto> weekly;
}
