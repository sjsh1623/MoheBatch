package com.example.ingestion.batch.reader;

import com.example.ingestion.dto.GooglePlaceDetail;
import com.example.ingestion.dto.NaverPlaceItem;

/**
 * Combined place data from Naver and Google APIs
 */
public class EnrichedPlace {
    private final NaverPlaceItem naverPlace;
    private final GooglePlaceDetail googlePlace;

    public EnrichedPlace(NaverPlaceItem naverPlace, GooglePlaceDetail googlePlace) {
        this.naverPlace = naverPlace;
        this.googlePlace = googlePlace;
    }

    public NaverPlaceItem getNaverPlace() {
        return naverPlace;
    }

    public GooglePlaceDetail getGooglePlace() {
        return googlePlace;
    }

    public boolean hasGoogleData() {
        return googlePlace != null;
    }

    @Override
    public String toString() {
        return String.format("EnrichedPlace{naver='%s', hasGoogle=%s}",
                           naverPlace != null ? naverPlace.getCleanTitle() : "null",
                           hasGoogleData());
    }
}