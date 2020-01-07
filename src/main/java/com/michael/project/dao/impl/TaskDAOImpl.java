package com.michael.project.dao.impl;

import com.michael.project.dao.ITaskDAO;
import com.michael.project.domain.Task;

/**
 * 任务管理DAO接口实现
 *
 * @author Michael Chu
 * @since 2020-01-07 14:36
 */
public class TaskDAOImpl implements ITaskDAO {

    /**
     * 根据主键查询任务
     *
     * @param taskId 主键
     * @return 任务
     */
    @Override
    public Task findById(long taskId) {
        final Task task = new Task();

        String sql = "select * from task where task_id = ?";
        Object[] params = new Object[]{taskId};


        return null;
    }
}
