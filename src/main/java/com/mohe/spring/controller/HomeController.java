package com.mohe.spring.controller;

import com.mohe.spring.dto.ApiResponse;
import com.mohe.spring.dto.PlaceDto;
import com.mohe.spring.service.PlaceService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.ExampleObject;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.security.SecurityRequirements;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/api/home")
@Tag(name = "홈 페이지", description = "홈 페이지 데이터 API")
public class HomeController {
    
    private final PlaceService placeService;
    
    public HomeController(PlaceService placeService) {
        this.placeService = placeService;
    }
    
    @GetMapping("/images")
    @SecurityRequirements
    @Operation(
        summary = "홈 페이지 이미지 데이터 가져오기",
        description = "홈 페이지에 표시할 장소 이미지들을 데이터베이스에서 가져옵니다."
    )
    @ApiResponses(
        value = {
            @io.swagger.v3.oas.annotations.responses.ApiResponse(
                responseCode = "200",
                description = "홈 페이지 이미지 조회 성공",
                content = @Content(
                    mediaType = "application/json",
                    examples = @ExampleObject(
                        value = """
                        {
                          "success": true,
                          "data": [
                            {
                              "id": 1,
                              "name": "카페 모헤",
                              "imageUrl": "https://example.com/image1.jpg",
                              "rating": 4.5,
                              "category": "카페",
                              "distance": 0
                            }
                          ],
                          "message": "홈 페이지 이미지를 성공적으로 조회했습니다."
                        }
                        """
                    )
                )
            ),
            @io.swagger.v3.oas.annotations.responses.ApiResponse(
                responseCode = "500",
                description = "서버 오류",
                content = @Content(
                    mediaType = "application/json",
                    examples = @ExampleObject(
                        value = """
                        {
                          "success": false,
                          "error": {
                            "code": "INTERNAL_SERVER_ERROR",
                            "message": "홈 페이지 데이터를 불러오는 중 오류가 발생했습니다."
                          }
                        }
                        """
                    )
                )
            )
        }
    )
    public ResponseEntity<ApiResponse<List<PlaceDto.PlaceResponse>>> getHomeImages(
            HttpServletRequest httpRequest) {
        try {
            // Get places with images from the database (limit to 20 for home page)
            List<PlaceDto.PlaceResponse> homeImages = placeService.getPlacesWithImages(20);
            
            // Set all distances to 0 as per requirements
            homeImages.forEach(place -> place.setDistance(0.0));
            
            return ResponseEntity.ok(ApiResponse.success(
                homeImages,
                "홈 페이지 이미지를 성공적으로 조회했습니다."
            ));
        } catch (Exception e) {
            return ResponseEntity.status(500).body(
                ApiResponse.error(
                    "INTERNAL_SERVER_ERROR",
                    e.getMessage() != null ? e.getMessage() : "홈 페이지 데이터를 불러오는 중 오류가 발생했습니다.",
                    httpRequest.getRequestURI()
                )
            );
        }
    }
}