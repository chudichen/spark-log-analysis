package com.michael.project.dao.impl;

import com.michael.project.dao.ISessionAggStatDAO;
import com.michael.project.domain.SessionAggStat;
import com.michael.project.jdbc.JDBCHelper;

/**
 * session聚合统计DAO实现类
 *
 * @author Michael Chu
 * @since 2020-01-11 17:26
 */
public class SessionAggStatDAOImpl implements ISessionAggStatDAO {

    /**
     * 插入session聚合统计结果
     *
     * @param sessionAggStat {@link SessionAggStat} 聚合统计结果
     */
    @Override
    public void insert(SessionAggStat sessionAggStat) {
        String sql = "insert into session_agg_stat "
                + "values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";

        Object[] params = new Object[]{sessionAggStat.getTaskId(),
                sessionAggStat.getSession_count(),
                sessionAggStat.getVisit_length_1s_3s_ratio(),
                sessionAggStat.getVisit_length_4s_6s_ratio(),
                sessionAggStat.getVisit_length_7s_9s_ratio(),
                sessionAggStat.getVisit_length_10s_30s_ratio(),
                sessionAggStat.getVisit_length_30s_60s_ratio(),
                sessionAggStat.getVisit_length_1m_3m_ratio(),
                sessionAggStat.getVisit_length_3m_10m_ratio(),
                sessionAggStat.getVisit_length_10m_30m_ratio(),
                sessionAggStat.getVisit_length_30m_ratio(),
                sessionAggStat.getStep_length_1_3_ratio(),
                sessionAggStat.getStep_length_4_6_ratio(),
                sessionAggStat.getStep_length_7_9_ratio(),
                sessionAggStat.getStep_length_10_30_ratio(),
                sessionAggStat.getStep_length_30_60_ratio(),
                sessionAggStat.getStep_length_60_ratio()};

        JDBCHelper jdbcHelper = JDBCHelper.getInstance();
        jdbcHelper.executeUpdate(sql, params);
    }
}
