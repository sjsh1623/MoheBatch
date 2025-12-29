package com.mohe.batch.exception;

/**
 * 크롤링 시 장소를 찾을 수 없는 경우 (404, 폐업 등) 발생하는 예외
 * 이 예외가 발생하면 해당 Place는 DB에서 삭제됨
 */
public class PlaceNotFoundException extends RuntimeException {

    private final Long placeId;
    private final String placeName;

    public PlaceNotFoundException(Long placeId, String placeName, String message) {
        super(message);
        this.placeId = placeId;
        this.placeName = placeName;
    }

    public Long getPlaceId() {
        return placeId;
    }

    public String getPlaceName() {
        return placeName;
    }
}
