package com.mohe.spring.entity

import jakarta.persistence.AttributeConverter
import jakarta.persistence.Converter

@Converter
class StringArrayConverter : AttributeConverter<List<String>, Array<String>> {
    
    override fun convertToDatabaseColumn(attribute: List<String>?): Array<String>? {
        return attribute?.toTypedArray()
    }
    
    override fun convertToEntityAttribute(dbData: Array<String>?): List<String> {
        return dbData?.toList() ?: emptyList()
    }
}