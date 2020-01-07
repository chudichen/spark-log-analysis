package com.michael.project.dao;

import com.michael.project.domain.Task;

/**
 * 任务管理DAO接口
 *
 * @author Michael Chu
 * @since 2020-01-07 14:33
 */
public interface ITaskDAO {

    /**
     * 根据主键查询任务
     *
     * @param taskId 主键
     * @return 任务
     */
    Task findById(long taskId);
}
