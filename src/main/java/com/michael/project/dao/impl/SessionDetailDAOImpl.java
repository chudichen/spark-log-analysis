package com.michael.project.dao.impl;

import com.michael.project.dao.ISessionDetailDAO;
import com.michael.project.domain.SessionDetail;
import com.michael.project.jdbc.JDBCHelper;

/**
 * session明细DAO实现类
 *
 * @author Michael Chu
 * @since 2020-01-14 14:39
 */
public class SessionDetailDAOImpl implements ISessionDetailDAO {

    /**
     * 插入一条session明细数据
     *
     * @param sessionDetail session
     */
    @Override
    public void insert(SessionDetail sessionDetail) {
        String sql = "insert into session_detail values(?,?,?,?,?,?,?,?,?,?,?,?)";

        Object[] params = new Object[]{
                sessionDetail.getTaskId(),
                sessionDetail.getUserId(),
                sessionDetail.getSessionId(),
                sessionDetail.getPageId(),
                sessionDetail.getActionTime(),
                sessionDetail.getSearchKeyword(),
                sessionDetail.getClickCategoryId(),
                sessionDetail.getClickProductId(),
                sessionDetail.getOrderCategoryIds(),
                sessionDetail.getOrderProductIds(),
                sessionDetail.getPayCategoryIds(),
                sessionDetail.getPayProductIds()};

        JDBCHelper jdbcHelper = JDBCHelper.getInstance();
        jdbcHelper.executeUpdate(sql, params);
    }

    @Override
    public void delete(long taskId) {
        String sql = "delete from session_detail where task_id = ?";
        Object[] params = {taskId};

        JDBCHelper jdbcHelper = JDBCHelper.getInstance();
        jdbcHelper.executeUpdate(sql, params);
    }
}
