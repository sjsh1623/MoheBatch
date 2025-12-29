package com.mohe.batch.dto.queue;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.time.LocalDateTime;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class TaskProgress {

    private String taskId;
    private Long placeId;
    private String status;  // pending, processing, completed, failed, retrying
    private String workerId;
    private LocalDateTime startTime;
    private LocalDateTime endTime;
    private int attempts;
    private String lastError;
    private boolean updateMenus;
    private boolean updateImages;
    private boolean updateReviews;
}
