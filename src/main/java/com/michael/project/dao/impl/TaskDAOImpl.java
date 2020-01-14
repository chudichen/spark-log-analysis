package com.michael.project.dao.impl;

import com.michael.project.dao.ITaskDAO;
import com.michael.project.domain.Task;
import com.michael.project.jdbc.JDBCHelper;

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

        JDBCHelper jdbcHelper = JDBCHelper.getInstance();
        jdbcHelper.executeQuery(sql, params, rs -> {
            if (rs.next()) {
                Long id = rs.getLong(1);
                String taskName = rs.getString(2);
                String createTime = rs.getString(3);
                String startTime = rs.getString(4);
                String finishTime = rs.getString(5);
                String taskType = rs.getString(6);
                String taskStatus = rs.getString(7);
                String taskParam = rs.getString(8);

                task.setTaskId(id);
                task.setTaskName(taskName);
                task.setCreateTime(createTime);
                task.setStartTime(startTime);
                task.setFinishTime(finishTime);
                task.setTaskType(taskType);
                task.setTaskStatus(taskStatus);
                task.setTaskParam(taskParam);
            }
        });
        return task;
    }

    @Override
    public void delete(long taskId) {
        String sql = "delete from task where task_id = ?";
        Object[] params = {taskId};

        JDBCHelper jdbcHelper = JDBCHelper.getInstance();
        jdbcHelper.executeUpdate(sql, params);
    }

}
