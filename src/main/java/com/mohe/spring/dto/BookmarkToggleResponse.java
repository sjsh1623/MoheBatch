package com.mohe.spring.dto;

public class BookmarkToggleResponse {
    private boolean isBookmarked;
    private String message;

    public BookmarkToggleResponse() {}

    public BookmarkToggleResponse(boolean isBookmarked, String message) {
        this.isBookmarked = isBookmarked;
        this.message = message;
    }

    public boolean isBookmarked() {
        return isBookmarked;
    }

    public void setBookmarked(boolean bookmarked) {
        isBookmarked = bookmarked;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }
}