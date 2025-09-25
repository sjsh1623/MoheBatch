package com.mohe.spring.dto;

import java.util.List;

public class BookmarkListResponse {
    private List<BookmarkData> bookmarks;

    public BookmarkListResponse() {}

    public BookmarkListResponse(List<BookmarkData> bookmarks) {
        this.bookmarks = bookmarks;
    }

    public List<BookmarkData> getBookmarks() {
        return bookmarks;
    }

    public void setBookmarks(List<BookmarkData> bookmarks) {
        this.bookmarks = bookmarks;
    }
}