package com.example.ingestion.batch.processor

import com.example.ingestion.dto.GovernmentRegionDto
import com.example.ingestion.entity.Place
import org.springframework.batch.item.ItemProcessor
import org.springframework.stereotype.Component

@Component
class GovernmentRegionProcessor : ItemProcessor<GovernmentRegionDto, Place> {

    override fun process(item: GovernmentRegionDto): Place? {
        val parts = item.locataddNm?.split(" ") ?: return null
        if (parts.size < 3) return null

        return Place(
            locatName = item.locataddNm!!,
            sido = item.sidoCd!!,
            sigungu = item.sigunguCd!!,
            dong = item.umdCd!!,
            latitude = null,
            longitude = null,
            naverData = null,
            googleData = null,
            name = item.locataddNm!!,
            description = null,
            imageUrl = null,
            tags = mutableListOf()
        )
    }
}
