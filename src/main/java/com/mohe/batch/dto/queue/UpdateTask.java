package com.mohe.batch.dto.queue;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.io.Serializable;
import java.time.LocalDateTime;
import java.util.UUID;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class UpdateTask implements Serializable {

    private String taskId;
    private Long placeId;
    private boolean updateMenus;
    private boolean updateImages;
    private boolean updateReviews;
    private int priority;  // 0=normal, 1=high
    private int attempts;
    private LocalDateTime createdAt;
    private LocalDateTime scheduledAt;

    public static UpdateTask create(Long placeId, boolean menus, boolean images, boolean reviews) {
        UpdateTask task = new UpdateTask();
        task.setTaskId(UUID.randomUUID().toString());
        task.setPlaceId(placeId);
        task.setUpdateMenus(menus);
        task.setUpdateImages(images);
        task.setUpdateReviews(reviews);
        task.setPriority(0);
        task.setAttempts(0);
        task.setCreatedAt(LocalDateTime.now());
        return task;
    }

    public static UpdateTask createWithPriority(Long placeId, boolean menus, boolean images, boolean reviews, int priority) {
        UpdateTask task = create(placeId, menus, images, reviews);
        task.setPriority(priority);
        return task;
    }
}
