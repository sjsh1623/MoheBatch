package com.mohe.spring.dto;

import java.util.List;

public class BookmarkListResponse {
    private List<BookmarkData> bookmarks;
    private int page;
    private int size;
    private int totalPages;
    private long totalElements;

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

    public int getPage() {
        return page;
    }

    public void setPage(int page) {
        this.page = page;
    }

    public int getSize() {
        return size;
    }

    public void setSize(int size) {
        this.size = size;
    }

    public int getTotalPages() {
        return totalPages;
    }

    public void setTotalPages(int totalPages) {
        this.totalPages = totalPages;
    }

    public long getTotalElements() {
        return totalElements;
    }

    public void setTotalElements(long totalElements) {
        this.totalElements = totalElements;
    }
}
