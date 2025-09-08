package com.mohe.spring.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * DTO for Korean Government Administrative Region Data
 * Fetched from Korean Government Administrative Standard Code API
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class KoreanRegionDto {
    
    @JsonProperty("region_cd")
    private String regionCode;
    
    @JsonProperty("locatadd_nm")
    private String locationName;
    
    @JsonProperty("sido_cd")
    private String sidoCode;
    
    @JsonProperty("sgg_cd")
    private String sigunguCode;
    
    @JsonProperty("umd_cd")
    private String umdCode;
    
    @JsonProperty("ri_cd")
    private String riCode;
    
    // Default constructor
    public KoreanRegionDto() {}
    
    // Getters and setters
    public String getRegionCode() {
        return regionCode;
    }
    
    public void setRegionCode(String regionCode) {
        this.regionCode = regionCode;
    }
    
    public String getLocationName() {
        return locationName;
    }
    
    public void setLocationName(String locationName) {
        this.locationName = locationName;
    }
    
    public String getSidoCode() {
        return sidoCode;
    }
    
    public void setSidoCode(String sidoCode) {
        this.sidoCode = sidoCode;
    }
    
    public String getSigunguCode() {
        return sigunguCode;
    }
    
    public void setSigunguCode(String sigunguCode) {
        this.sigunguCode = sigunguCode;
    }
    
    public String getUmdCode() {
        return umdCode;
    }
    
    public void setUmdCode(String umdCode) {
        this.umdCode = umdCode;
    }
    
    public String getRiCode() {
        return riCode;
    }
    
    public void setRiCode(String riCode) {
        this.riCode = riCode;
    }
    
    /**
     * Check if this is a dong-level region (umdCode != "00" and riCode == "00")
     */
    public boolean isDongLevel() {
        return umdCode != null && !umdCode.equals("00") && 
               riCode != null && riCode.equals("00");
    }
    
    /**
     * Extract simple location name (e.g., "신사동" from "서울특별시 강남구 신사동")
     */
    public String getSimpleLocationName() {
        if (locationName == null) return null;
        
        String[] parts = locationName.split(" ");
        if (parts.length >= 3) {
            return parts[2]; // Return the dong/gu part
        }
        return parts.length > 0 ? parts[parts.length - 1] : locationName;
    }
    
    @Override
    public String toString() {
        return "KoreanRegionDto{" +
                "regionCode='" + regionCode + '\'' +
                ", locationName='" + locationName + '\'' +
                ", sidoCode='" + sidoCode + '\'' +
                ", sigunguCode='" + sigunguCode + '\'' +
                ", umdCode='" + umdCode + '\'' +
                ", riCode='" + riCode + '\'' +
                '}';
    }
}