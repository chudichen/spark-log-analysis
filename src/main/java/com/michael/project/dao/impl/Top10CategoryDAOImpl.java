package com.michael.project.dao.impl;

import com.michael.project.dao.ITop10CategoryDAO;
import com.michael.project.domain.Top10Category;
import com.michael.project.jdbc.JDBCHelper;

/**
 * top10品类DAO实现
 *
 * @author Michael Chu
 * @since 2020-01-16 15:45
 */
public class Top10CategoryDAOImpl implements ITop10CategoryDAO {

    @Override
    public void insert(Top10Category category) {
        String sql = "insert into top10_category (`task_id`, `category_id`, `click_count`, `order_count`, `pay_count`) values(?,?,?,?,?)";

        Object[] params = new Object[]{
                category.getTaskId(),
                category.getCategoryId(),
                category.getClickCount(),
                category.getOrderCount(),
                category.getPayCount()};

        JDBCHelper jdbcHelper = JDBCHelper.getInstance();
        jdbcHelper.executeUpdate(sql, params);
    }

    @Override
    public void delete(long taskId) {
        String sql = "delete from top10_category where task_id = ?";
        Object[] params = {taskId};

        JDBCHelper jdbcHelper = JDBCHelper.getInstance();
        jdbcHelper.executeUpdate(sql, params);
    }
}
