package com.mohe.batch.dto.crawling;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class WeeklyHoursDto {
    private String open;
    private String close;
    private String description;
    private boolean isOperating;
}
