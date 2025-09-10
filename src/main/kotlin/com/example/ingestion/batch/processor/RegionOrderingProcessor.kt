package com.example.ingestion.batch.processor

import com.example.ingestion.entity.Place
import org.springframework.batch.item.ItemProcessor
import org.springframework.stereotype.Component

@Component
class RegionOrderingProcessor : ItemProcessor<List<Place>, List<Place>> {

    private val order = listOf("서울특별시", "경기도", "제주특별자치도", "부산광역시")

    override fun process(items: List<Place>): List<Place> {
        return items.sortedWith(compareBy { 
            val sido = it.sido
            val index = order.indexOf(sido)
            if (index != -1) {
                index
            } else {
                order.size
            }
        })
    }
}
