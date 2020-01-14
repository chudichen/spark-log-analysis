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

    private long taskId;
    private long userId;
    private String sessionId;
    private long pageId;
    private String actionTime;
    private String searchKeyword;
    private long clickCategoryId;
    private long clickProductId;
    private String orderCategoryIds;
    private String orderProductIds;
    private String payCategoryIds;
    private String payProductIds;

}
