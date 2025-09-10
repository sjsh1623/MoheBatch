package com.example.ingestion.batch.reader

import com.example.ingestion.dto.GovernmentRegionDto
import org.springframework.batch.item.ItemReader
import org.springframework.stereotype.Component
import org.springframework.web.reactive.function.client.WebClient
import org.springframework.web.reactive.function.client.bodyToMono

import org.springframework.beans.factory.annotation.Qualifier

@Component
class GovernmentApiReader(
    @Qualifier("externalApiWebClient") private val webClient: WebClient
) : ItemReader<GovernmentRegionDto> {

    private val serviceKey = "f5a3bcde8e1b032f6f0d36b525353d3e6b3843e9d4a478728219054bde74f20f"
    private var regions: List<GovernmentRegionDto> = emptyList()
    private var index = 0
    private val order = listOf("서울특별시", "경기도", "제주특별자치도", "부산광역시")

    override fun read(): GovernmentRegionDto? {
        if (regions.isEmpty()) {
            regions = fetchAllRegions()
            index = 0
        }

        return if (index < regions.size) {
            regions[index++]
        } else {
            null
        }
    }

    fun fetchAllRegionsOnce(): List<GovernmentRegionDto> {
        if (regions.isEmpty()) {
            regions = fetchAllRegions()
        }
        return regions
    }

    private fun fetchAllRegions(): List<GovernmentRegionDto> {
        val allRegions = mutableListOf<GovernmentRegionDto>()
        var page = 1
        
        println("🚀 Starting Korean Government API data fetch...")
        
        while (true) {
            println("📖 Fetching page $page with 1000 records per page...")
            
            val response = webClient.get()
                .uri {
                    it.scheme("http")
                        .host("apis.data.go.kr")
                        .path("/1741000/StanReginCd/getStanReginCdList")
                        .queryParam("serviceKey", serviceKey)
                        .queryParam("pageNo", page)
                        .queryParam("numOfRows", 1000)
                        .queryParam("type", "json")
                        .build()
                }
                .retrieve()
                .bodyToMono<String>()
                .map { jsonString ->
                    // Parse the JSON string manually since the API returns text/html content type
                    com.fasterxml.jackson.databind.ObjectMapper().readValue(jsonString, Map::class.java) as Map<String, Any>
                }
                .block()

            val stanReginCd = response?.get("StanReginCd") as? List<Map<String, Any>>
            val rows = stanReginCd?.get(1)?.get("row") as? List<Map<String, Any>>

            if (rows == null || rows.isEmpty()) {
                println("📄 Page $page returned no data - pagination complete")
                break
            }

            val rawCount = rows.size
            val newRegions = rows.map {
                GovernmentRegionDto(
                    sidoCd = it["sido_cd"] as? String,
                    sigunguCd = it["sgg_cd"] as? String,
                    umdCd = it["umd_cd"] as? String,
                    riCd = it["ri_cd"] as? String,
                    locataddNm = it["locatadd_nm"] as? String
                )
            }.filter { it.umdCd != "00" && it.riCd == "00" }
            
            val filteredCount = newRegions.size
            allRegions.addAll(newRegions)
            
            println("📊 Page $page: $rawCount raw records → $filteredCount filtered records (Total so far: ${allRegions.size})")
            
            // Check if we got less than 1000 records, meaning we've reached the end
            if (rawCount < 1000) {
                println("📄 Page $page returned only $rawCount records - reached end of data")
                break
            }
            
            page++
        }
        
        println("✅ Korean Government API fetch complete: ${allRegions.size} dong-level regions loaded")
        
        // Check for total count from the first page if available
        val firstPageResponse = webClient.get()
            .uri {
                it.scheme("http")
                    .host("apis.data.go.kr")
                    .path("/1741000/StanReginCd/getStanReginCdList")
                    .queryParam("serviceKey", serviceKey)
                    .queryParam("pageNo", 1)
                    .queryParam("numOfRows", 1000)
                    .queryParam("type", "json")
                    .build()
            }
            .retrieve()
            .bodyToMono<String>()
            .map { jsonString ->
                com.fasterxml.jackson.databind.ObjectMapper().readValue(jsonString, Map::class.java) as Map<String, Any>
            }
            .block()
            
        val firstStanReginCd = firstPageResponse?.get("StanReginCd") as? List<Map<String, Any>>
        val totalCountInfo = firstStanReginCd?.get(0)?.get("head") as? List<Map<String, Any>>
        val totalCount = totalCountInfo?.get(0)?.get("totalCount") as? Int
        
        if (totalCount != null) {
            println("📈 Government API reports total of $totalCount administrative regions")
            println("🎯 Processed ${allRegions.size} dong-level regions (filtered from $totalCount total)")
        }

        return allRegions.sortedWith(compareBy { 
            val sido = it.locataddNm?.split(" ")?.get(0)
            val index = order.indexOf(sido)
            if (index != -1) {
                index
            } else {
                order.size
            }
        })
    }
}
