package com.mohe.spring.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import java.util.Map;

/**
 * Response DTOs for Korean Government Administrative Standard Code API
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class KoreanRegionResponse {
    
    @JsonProperty("StanReginCd")
    private List<StanReginCdItem> stanReginCd;
    
    public List<StanReginCdItem> getStanReginCd() {
        return stanReginCd;
    }
    
    public void setStanReginCd(List<StanReginCdItem> stanReginCd) {
        this.stanReginCd = stanReginCd;
    }
    
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class StanReginCdItem {
        
        @JsonProperty("head")
        private List<Map<String, Object>> head;
        
        @JsonProperty("row")
        private List<KoreanRegionDto> row;
        
        public List<Map<String, Object>> getHead() {
            return head;
        }
        
        public void setHead(List<Map<String, Object>> head) {
            this.head = head;
        }
        
        public List<KoreanRegionDto> getRow() {
            return row;
        }
        
        public void setRow(List<KoreanRegionDto> row) {
            this.row = row;
        }
        
        /**
         * Extract total count from head section
         */
        public Integer getTotalCount() {
            if (head != null && !head.isEmpty()) {
                Map<String, Object> headInfo = head.get(0);
                Object totalCount = headInfo.get("totalCount");
                if (totalCount instanceof Integer) {
                    return (Integer) totalCount;
                }
                if (totalCount instanceof String) {
                    try {
                        return Integer.parseInt((String) totalCount);
                    } catch (NumberFormatException e) {
                        return null;
                    }
                }
            }
            return null;
        }
    }
}