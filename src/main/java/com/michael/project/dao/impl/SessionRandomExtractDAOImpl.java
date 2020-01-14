package com.michael.project.dao.impl;

import com.michael.project.dao.ISessionRandomExtractDAO;
import com.michael.project.domain.SessionRandomExtract;
import com.michael.project.jdbc.JDBCHelper;

/**
 * 随机抽取session的DAO实现
 *
 * @author Michael Chu
 * @since 2020-01-14 10:16
 */
public class SessionRandomExtractDAOImpl implements ISessionRandomExtractDAO {

    /**
     * 插入session随机抽取
     *
     * @param sessionRandomExtract session
     */
    @Override
    public void insert(SessionRandomExtract sessionRandomExtract) {
        String sql = "insert into session_random_extract values (?,?,?,?,?)";

        Object[] params = new Object[]{
                sessionRandomExtract.getTaskId(),
                sessionRandomExtract.getSessionId(),
                sessionRandomExtract.getStartTime(),
                sessionRandomExtract.getSearchKeywords(),
                sessionRandomExtract.getClickCategoryIds()
        };

        JDBCHelper jdbcHelper = JDBCHelper.getInstance();
        jdbcHelper.executeUpdate(sql, params);
    }

    @Override
    public void delete(long taskId) {
        String sql = "delete from session_random_extract where task_id = ?";
        Object[] params = {taskId};

        JDBCHelper jdbcHelper = JDBCHelper.getInstance();
        jdbcHelper.executeUpdate(sql, params);
    }

}
