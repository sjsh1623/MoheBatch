package com.mohe.spring.controller

import com.mohe.spring.dto.*
import com.mohe.spring.service.BookmarkService
import io.swagger.v3.oas.annotations.Operation
import io.swagger.v3.oas.annotations.Parameter
import io.swagger.v3.oas.annotations.media.Content
import io.swagger.v3.oas.annotations.media.ExampleObject
import io.swagger.v3.oas.annotations.media.Schema
import io.swagger.v3.oas.annotations.responses.ApiResponse as SwaggerApiResponse
import io.swagger.v3.oas.annotations.responses.ApiResponses
import io.swagger.v3.oas.annotations.security.SecurityRequirement
import io.swagger.v3.oas.annotations.tags.Tag
import jakarta.servlet.http.HttpServletRequest
import jakarta.validation.Valid
import org.springframework.http.ResponseEntity
import org.springframework.security.access.prepost.PreAuthorize
import org.springframework.web.bind.annotation.*

@RestController
@RequestMapping("/api/bookmarks")
@PreAuthorize("hasRole('USER')")
@SecurityRequirement(name = "bearerAuth")
@Tag(name = "북마크 관리", description = "사용자 장소 북마크 관리 API")
class BookmarkController(
    private val bookmarkService: BookmarkService
) {
    
    @PostMapping("/toggle")
    @Operation(
        summary = "북마크 추가/제거",
        description = "지정된 장소의 북마크를 추가하거나 제거합니다."
    )
    @ApiResponses(
        value = [
            SwaggerApiResponse(
                responseCode = "200",
                description = "북마크 처리 성공",
                content = [Content(
                    mediaType = "application/json",
                    schema = Schema(implementation = BookmarkToggleResponse::class),
                    examples = [ExampleObject(
                        value = """
                        {
                          "success": true,
                          "data": {
                            "isBookmarked": true,
                            "message": "북마크가 추가되었습니다."
                          }
                        }
                        """
                    )]
                )]
            ),
            SwaggerApiResponse(
                responseCode = "404",
                description = "장소를 찾을 수 없음"
            )
        ]
    )
    fun toggleBookmark(
        @Parameter(description = "북마크 토글 요청", required = true)
        @Valid @RequestBody request: BookmarkToggleRequest,
        httpRequest: HttpServletRequest
    ): ResponseEntity<ApiResponse<BookmarkToggleResponse>> {
        return try {
            val response = bookmarkService.toggleBookmark(request)
            ResponseEntity.ok(ApiResponse.success(response))
        } catch (e: Exception) {
            val errorCode = when {
                e.message?.contains("찾을 수 없습니다") == true -> ErrorCode.RESOURCE_NOT_FOUND
                else -> ErrorCode.INTERNAL_SERVER_ERROR
            }
            ResponseEntity.badRequest().body(
                ApiResponse.error(
                    code = errorCode,
                    message = e.message ?: "북마크 처리에 실패했습니다",
                    path = httpRequest.requestURI
                )
            )
        }
    }
    
    @GetMapping
    @Operation(
        summary = "북마크 목록 조회",
        description = "사용자가 북마크한 장소 목록을 조회합니다."
    )
    @ApiResponses(
        value = [
            SwaggerApiResponse(
                responseCode = "200",
                description = "북마크 목록 조회 성공",
                content = [Content(
                    mediaType = "application/json",
                    schema = Schema(implementation = BookmarkListResponse::class),
                    examples = [ExampleObject(
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
                    )]
                )]
            )
        ]
    )
    fun getBookmarks(
        httpRequest: HttpServletRequest
    ): ResponseEntity<ApiResponse<BookmarkListResponse>> {
        return try {
            val response = bookmarkService.getBookmarks()
            ResponseEntity.ok(ApiResponse.success(response))
        } catch (e: Exception) {
            ResponseEntity.badRequest().body(
                ApiResponse.error(
                    code = ErrorCode.INTERNAL_SERVER_ERROR,
                    message = e.message ?: "북마크 목록 조회에 실패했습니다",
                    path = httpRequest.requestURI
                )
            )
        }
    }
}