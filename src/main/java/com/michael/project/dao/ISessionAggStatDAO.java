package com.michael.project.dao;

import com.michael.project.domain.SessionAggStat;

/**
 * session聚合统计模块DAO接口
 *
 * @author Michael Chu
 * @since 2020-01-11 17:22
 */
public interface ISessionAggStatDAO {

    /**
     * 插入session聚合统计结果
     *
     * @param sessionAggStat {@link SessionAggStat} 聚合统计结果
     */
    void insert(SessionAggStat sessionAggStat);
}
