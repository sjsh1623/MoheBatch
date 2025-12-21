package com.mohe.batch.dto.crawling;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class MenuItemDto {
    private String name;
    private String price;
    private String description;

    @JsonProperty("image_url")
    private String imageUrl;
}
