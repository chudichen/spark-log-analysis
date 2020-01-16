package com.michael.project.domain;

import lombok.Data;

/**
 * session明细
 *
 * @author Michael Chu
 * @since 2020-01-14 14:37
 */
@Data
public class SessionDetail {

    private Long id;
    private Long taskId;
    private Long userId;
    private String sessionId;
    private Long pageId;
    private String actionTime;
    private String searchKeyword;
    private Long clickCategoryId;
    private Long clickProductId;
    private String orderCategoryIds;
    private String orderProductIds;
    private String payCategoryIds;
    private String payProductIds;

}
