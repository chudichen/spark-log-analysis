package com.michael.project.domain;

import lombok.Data;

/**
 * top10品类
 *
 * @author Michael Chu
 * @since 2020-01-16 15:41
 */
@Data
public class Top10Category {

    private Long taskId;
    private Long categoryId;
    private Long clickCount;
    private Long orderCount;
    private Long payCount;
}
