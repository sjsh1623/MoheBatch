package com.mohe.batch.dto.crawling;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@JsonIgnoreProperties(ignoreUnknown = true)
public class WeeklyHoursDto {
    private String open;
    private String close;
    private String description;

    @JsonProperty("isOperating")
    private boolean operating;
}
