package com.mohe.spring.dto;

public class BookmarkStatusResponse {
    private boolean isBookmarked;

    public BookmarkStatusResponse() {
    }

    public BookmarkStatusResponse(boolean isBookmarked) {
        this.isBookmarked = isBookmarked;
    }

    public boolean isBookmarked() {
        return isBookmarked;
    }

    public void setBookmarked(boolean bookmarked) {
        isBookmarked = bookmarked;
    }
}
