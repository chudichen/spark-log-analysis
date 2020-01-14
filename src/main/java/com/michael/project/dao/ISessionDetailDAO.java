package com.michael.project.dao;

import com.michael.project.domain.SessionDetail;

/**
 * session明细接口
 *
 * @author Michael Chu
 * @since 2020-01-14 14:38
 */
public interface ISessionDetailDAO {

    /**
     * 插入一条session明细数据
     *
     * @param sessionDetail session
     */
    void insert(SessionDetail sessionDetail);

    /**
     * 通过taskId删除
     *
     * @param taskId 任务ID
     */
    void delete(long taskId);
}
