package com.mohe.batch.dto.crawling;

import lombok.Getter;
import lombok.Setter;

import java.util.List;

@Getter
@Setter
public class MenuDataDto {
    private String name;
    private List<MenuItemDto> menus;
}
