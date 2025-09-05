package com.mohe.spring.service;

/**
 * Address information data class
 */
public class AddressInfo {
    private final String fullAddress;
    private final String shortAddress;
    private final String sido;           // 시/도 (서울특별시, 경기도 등)
    private final String sigungu;        // 시/군/구 (강남구, 성남시 등)  
    private final String dong;           // 동/면/읍 (역삼동, 분당동 등)
    private final String roadName;       // 도로명 (테헤란로, 판교로 등)
    private final String buildingNumber; // 건물번호
    private final double latitude;
    private final double longitude;
    
    public AddressInfo(String fullAddress, String shortAddress, String sido, String sigungu,
                      String dong, String roadName, String buildingNumber, double latitude, double longitude) {
        this.fullAddress = fullAddress;
        this.shortAddress = shortAddress;
        this.sido = sido;
        this.sigungu = sigungu;
        this.dong = dong;
        this.roadName = roadName;
        this.buildingNumber = buildingNumber;
        this.latitude = latitude;
        this.longitude = longitude;
    }
    
    // Getters
    public String getFullAddress() { return fullAddress; }
    public String getShortAddress() { return shortAddress; }
    public String getSido() { return sido; }
    public String getSigungu() { return sigungu; }
    public String getDong() { return dong; }
    public String getRoadName() { return roadName; }
    public String getBuildingNumber() { return buildingNumber; }
    public double getLatitude() { return latitude; }
    public double getLongitude() { return longitude; }
}