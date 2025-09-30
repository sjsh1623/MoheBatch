package com.mohe.spring.dto;

import java.time.OffsetDateTime;
import java.util.List;

/**
 * Summary DTO returned from the terms list endpoint.
 * This is intentionally simple so that the real agreement content can be
 * supplied later without changing the client contract.
 */
public class TermsListResponse {

    private List<TermsSummary> terms;

    public TermsListResponse() {
    }

    public TermsListResponse(List<TermsSummary> terms) {
        this.terms = terms;
    }

    public List<TermsSummary> getTerms() {
        return terms;
    }

    public void setTerms(List<TermsSummary> terms) {
        this.terms = terms;
    }

    /**
     * Lightweight view of a single agreement.
     */
    public static class TermsSummary {
        private String id;
        private String title;
        private boolean required;
        private boolean hasDetails;

        public TermsSummary() {
        }

        public TermsSummary(String id, String title, boolean required, boolean hasDetails) {
            this.id = id;
            this.title = title;
            this.required = required;
            this.hasDetails = hasDetails;
        }

        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }

        public String getTitle() {
            return title;
        }

        public void setTitle(String title) {
            this.title = title;
        }

        public boolean isRequired() {
            return required;
        }

        public void setRequired(boolean required) {
            this.required = required;
        }

        public boolean isHasDetails() {
            return hasDetails;
        }

        public void setHasDetails(boolean hasDetails) {
            this.hasDetails = hasDetails;
        }
    }

    /**
     * Detailed payload returned for an individual agreement.
     */
    public static class TermsDetailResponse {
        private String id;
        private String title;
        private String content;
        private OffsetDateTime lastUpdated;

        public TermsDetailResponse() {
        }

        public TermsDetailResponse(String id, String title, String content, OffsetDateTime lastUpdated) {
            this.id = id;
            this.title = title;
            this.content = content;
            this.lastUpdated = lastUpdated;
        }

        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }

        public String getTitle() {
            return title;
        }

        public void setTitle(String title) {
            this.title = title;
        }

        public String getContent() {
            return content;
        }

        public void setContent(String content) {
            this.content = content;
        }

        public OffsetDateTime getLastUpdated() {
            return lastUpdated;
        }

        public void setLastUpdated(OffsetDateTime lastUpdated) {
            this.lastUpdated = lastUpdated;
        }
    }
}
