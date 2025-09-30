package com.mohe.spring.controller;

import com.mohe.spring.dto.*;
import com.mohe.spring.service.BookmarkService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.ExampleObject;
import io.swagger.v3.oas.annotations.media.Schema;
// Import removed - using fully qualified name for ApiResponse to avoid conflict
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.security.SecurityRequirement;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.validation.Valid;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/bookmarks")
@PreAuthorize("hasRole('USER')")
@SecurityRequirement(name = "bearerAuth")
@Tag(name = "북마크 관리", description = "사용자 장소 북마크 관리 API")
public class BookmarkController {
    
    private final BookmarkService bookmarkService;
    
    public BookmarkController(BookmarkService bookmarkService) {
        this.bookmarkService = bookmarkService;
    }
    
    @PostMapping("/toggle")
    @Operation(
        summary = "북마크 추가/제거",
        description = "지정된 장소의 북마크를 추가하거나 제거합니다."
    )
    @ApiResponses(
        value = {
            @io.swagger.v3.oas.annotations.responses.ApiResponse(
                responseCode = "200",
                description = "북마크 처리 성공",
                content = @Content(
                    mediaType = "application/json",
                    schema = @Schema(implementation = BookmarkToggleResponse.class),
                    examples = @ExampleObject(
                        value = """
                        {
                          "success": true,
                          "data": {
                            "isBookmarked": true,
                            "message": "북마크가 추가되었습니다."
                          }
                        }
                        """
                    )
                )
            ),
            @io.swagger.v3.oas.annotations.responses.ApiResponse(
                responseCode = "404",
                description = "장소를 찾을 수 없음"
            )
        }
    )
    public ResponseEntity<ApiResponse<BookmarkToggleResponse>> toggleBookmark(
            @Parameter(description = "북마크 토글 요청", required = true)
            @Valid @RequestBody BookmarkToggleRequest request,
            HttpServletRequest httpRequest) {
        try {
            BookmarkToggleResponse response = bookmarkService.toggleBookmark(request);
            return ResponseEntity.ok(ApiResponse.success(response));
        } catch (Exception e) {
            String errorCode;
            if (e.getMessage() != null && e.getMessage().contains("찾을 수 없습니다")) {
                errorCode = ErrorCode.RESOURCE_NOT_FOUND;
            } else {
                errorCode = ErrorCode.INTERNAL_SERVER_ERROR;
            }
            ApiResponse<BookmarkToggleResponse> errorResponse = ApiResponse.error(
                errorCode,
                e.getMessage() != null ? e.getMessage() : "북마크 처리에 실패했습니다",
                httpRequest.getRequestURI()
            );
            return ResponseEntity.badRequest().body(errorResponse);
        }
    }

    @PostMapping
    @Operation(
        summary = "북마크 추가",
        description = "지정된 장소를 북마크 목록에 추가합니다."
    )
    public ResponseEntity<ApiResponse<BookmarkToggleResponse>> addBookmark(
            @Parameter(description = "북마크 요청", required = true)
            @Valid @RequestBody BookmarkToggleRequest request,
            HttpServletRequest httpRequest) {
        try {
            BookmarkToggleResponse response = bookmarkService.addBookmark(request.getPlaceId());
            return ResponseEntity.ok(ApiResponse.success(response));
        } catch (Exception e) {
            ApiResponse<BookmarkToggleResponse> errorResponse = ApiResponse.error(
                ErrorCode.INTERNAL_SERVER_ERROR,
                e.getMessage() != null ? e.getMessage() : "북마크 추가에 실패했습니다",
                httpRequest.getRequestURI()
            );
            return ResponseEntity.badRequest().body(errorResponse);
        }
    }

    @DeleteMapping("/{placeId}")
    @Operation(
        summary = "북마크 제거",
        description = "지정된 장소를 북마크 목록에서 제거합니다."
    )
    public ResponseEntity<ApiResponse<BookmarkToggleResponse>> removeBookmark(
            @Parameter(description = "장소 ID", required = true, example = "1")
            @PathVariable String placeId,
            HttpServletRequest httpRequest) {
        try {
            BookmarkToggleResponse response = bookmarkService.removeBookmark(placeId);
            return ResponseEntity.ok(ApiResponse.success(response));
        } catch (Exception e) {
            ApiResponse<BookmarkToggleResponse> errorResponse = ApiResponse.error(
                ErrorCode.INTERNAL_SERVER_ERROR,
                e.getMessage() != null ? e.getMessage() : "북마크 제거에 실패했습니다",
                httpRequest.getRequestURI()
            );
            return ResponseEntity.badRequest().body(errorResponse);
        }
    }

    @GetMapping
    @Operation(
        summary = "북마크 목록 조회",
        description = "사용자가 북마크한 장소 목록을 조회합니다."
    )
    @ApiResponses(
        value = {
            @io.swagger.v3.oas.annotations.responses.ApiResponse(
                responseCode = "200",
                description = "북마크 목록 조회 성공",
                content = @Content(
                    mediaType = "application/json",
                    schema = @Schema(implementation = BookmarkListResponse.class),
                    examples = @ExampleObject(
                        value = """
                        {
                          "success": true,
                          "data": {
                            "bookmarks": [
                              {
                                "id": "1",
                                "place": {
                                  "id": "1",
                                  "name": "카페 무브먼트랩",
                                  "location": "서울 성수동",
                                  "image": "https://example.com/place.jpg",
                                  "rating": 4.7
                                },
                                "createdAt": "2024-01-01T00:00:00Z"
                              }
                            ]
                          }
                        }
                        """
                    )
                )
            )
        }
    )
    public ResponseEntity<ApiResponse<BookmarkListResponse>> getBookmarks(
            @Parameter(description = "페이지 번호 (0부터 시작)", example = "0")
            @RequestParam(defaultValue = "0") int page,
            @Parameter(description = "페이지 크기", example = "10")
            @RequestParam(defaultValue = "10") int size,
            HttpServletRequest httpRequest) {
        try {
            BookmarkListResponse response = bookmarkService.getBookmarks(page, size);
            return ResponseEntity.ok(ApiResponse.success(response));
        } catch (Exception e) {
            ApiResponse<BookmarkListResponse> errorResponse = ApiResponse.error(
                ErrorCode.INTERNAL_SERVER_ERROR,
                e.getMessage() != null ? e.getMessage() : "북마크 목록 조회에 실패했습니다",
                httpRequest.getRequestURI()
            );
            return ResponseEntity.badRequest().body(errorResponse);
        }
    }

    @GetMapping("/{placeId}")
    @Operation(
        summary = "북마크 여부 확인",
        description = "지정된 장소가 현재 사용자에 의해 북마크되어 있는지 확인합니다."
    )
    public ResponseEntity<ApiResponse<BookmarkStatusResponse>> getBookmarkStatus(
            @Parameter(description = "장소 ID", required = true, example = "1")
            @PathVariable String placeId,
            HttpServletRequest httpRequest) {
        try {
            BookmarkStatusResponse response = bookmarkService.getBookmarkStatus(placeId);
            return ResponseEntity.ok(ApiResponse.success(response));
        } catch (Exception e) {
            ApiResponse<BookmarkStatusResponse> errorResponse = ApiResponse.error(
                ErrorCode.INTERNAL_SERVER_ERROR,
                e.getMessage() != null ? e.getMessage() : "북마크 상태 조회에 실패했습니다",
                httpRequest.getRequestURI()
            );
            return ResponseEntity.badRequest().body(errorResponse);
        }
    }
}
