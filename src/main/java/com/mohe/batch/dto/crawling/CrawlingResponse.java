package com.mohe.batch.dto.crawling;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class CrawlingResponse<T> {
    private boolean success;
    private String message;
    private T data;
}
