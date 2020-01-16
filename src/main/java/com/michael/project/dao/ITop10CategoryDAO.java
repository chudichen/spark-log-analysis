package com.michael.project.dao;

import com.michael.project.domain.Top10Category;

/**
 * top10品类DAO接口
 *
 * @author Michael Chu
 * @since 2020-01-16 15:43
 */
public interface ITop10CategoryDAO {

    /**
     * 插入热门分类
     *
     * @param category session
     */
    void insert(Top10Category category);

    /**
     * 通过taskId删除
     *
     * @param taskId 任务ID
     */
    void delete(long taskId);
}
