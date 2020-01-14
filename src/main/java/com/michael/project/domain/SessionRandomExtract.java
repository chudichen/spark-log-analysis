package com.michael.project.domain;

import lombok.Data;

/**
 * 随机抽取的session
 *
 * @author Michael Chu
 * @since 2020-01-14 10:14
 */
@Data
public class SessionRandomExtract {

    private Long taskId;
    private String sessionId;
    private String startTime;
    private String searchKeywords;
    private String clickCategoryIds;

}
