package com.michael.project.domain;

import lombok.Data;

import java.io.Serializable;

/**
 * 任务
 *
 * @author Michael Chu
 * @since 2020-01-07 14:34
 */
@Data
public class Task implements Serializable {

    private static final long serialVersionUID = 3518776796426921776L;

    private Long taskId;
    private String taskName;
    private String createTime;
    private String startTime;
    private String finishTime;
    private String taskType;
    private String taskStatus;
    private String taskParam;
}
