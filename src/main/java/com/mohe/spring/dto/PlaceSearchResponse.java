package com.mohe.spring.dto;

import java.util.List;
import java.util.Map;

public class PlaceSearchResponse {
    private List<PlaceDto> searchResults;
    private Map<String, Object> searchContext;
    private int totalCount;
    private String query;

    public PlaceSearchResponse() {}

    public PlaceSearchResponse(List<PlaceDto> searchResults, Map<String, Object> searchContext, int totalCount, String query) {
        this.searchResults = searchResults;
        this.searchContext = searchContext;
        this.totalCount = totalCount;
        this.query = query;
    }

    public List<PlaceDto> getSearchResults() {
        return searchResults;
    }

    public void setSearchResults(List<PlaceDto> searchResults) {
        this.searchResults = searchResults;
    }

    public Map<String, Object> getSearchContext() {
        return searchContext;
    }

    public void setSearchContext(Map<String, Object> searchContext) {
        this.searchContext = searchContext;
    }

    public int getTotalCount() {
        return totalCount;
    }

    public void setTotalCount(int totalCount) {
        this.totalCount = totalCount;
    }

    public String getQuery() {
        return query;
    }

    public void setQuery(String query) {
        this.query = query;
    }
}