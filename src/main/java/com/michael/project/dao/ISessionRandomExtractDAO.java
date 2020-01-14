package com.michael.project.dao;

import com.michael.project.domain.SessionRandomExtract;

/**
 * session随机抽取模块DAO接口
 *
 * @author Michael Chu
 * @since 2020-01-14 10:13
 */
public interface ISessionRandomExtractDAO {

    /**
     * 插入session随机抽取
     *
     * @param sessionRandomExtract session
     */
    void insert(SessionRandomExtract sessionRandomExtract);

    /**
     * 通过taskId删除
     *
     * @param taskId 任务ID
     */
    void delete(long taskId);
}
