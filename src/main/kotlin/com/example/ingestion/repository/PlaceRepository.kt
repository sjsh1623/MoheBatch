package com.example.ingestion.repository

import com.example.ingestion.entity.Place
import org.springframework.data.jpa.repository.JpaRepository
import org.springframework.data.jpa.repository.Modifying
import org.springframework.data.jpa.repository.Query
import org.springframework.data.repository.query.Param
import org.springframework.stereotype.Repository

@Repository
interface PlaceRepository : JpaRepository<Place, Long> {

    @Modifying
    @Query(
        value = """
        INSERT INTO places (locat_name, sido, sigungu, dong, latitude, longitude, naver_data, google_data, updated_at)
        VALUES (:locatName, :sido, :sigungu, :dong, :latitude, :longitude, CAST(:naverData AS jsonb), CAST(:googleData AS jsonb), NOW())
        ON CONFLICT (lower(TRIM(BOTH FROM sido)), lower(TRIM(BOTH FROM sigungu)), lower(TRIM(BOTH FROM dong))) DO UPDATE SET
            locat_name = EXCLUDED.locat_name,
            updated_at = NOW()
        """,
        nativeQuery = true
    )
    fun upsert(
        @Param("locatName") locatName: String,
        @Param("sido") sido: String,
        @Param("sigungu") sigungu: String,
        @Param("dong") dong: String,
        @Param("latitude") latitude: Double?,
        @Param("longitude") longitude: Double?,
        @Param("naverData") naverData: String?,
        @Param("googleData") googleData: String?
    ): Int
}
