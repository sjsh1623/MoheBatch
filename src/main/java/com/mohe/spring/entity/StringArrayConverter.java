package com.mohe.spring.entity;

import jakarta.persistence.AttributeConverter;
import jakarta.persistence.Converter;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

@Converter
public class StringArrayConverter implements AttributeConverter<List<String>, String[]> {
    
    @Override
    public String[] convertToDatabaseColumn(List<String> attribute) {
        if (attribute == null) {
            return null;
        }
        return attribute.toArray(new String[0]);
    }
    
    @Override
    public List<String> convertToEntityAttribute(String[] dbData) {
        if (dbData == null) {
            return Collections.emptyList();
        }
        return Arrays.asList(dbData);
    }
}