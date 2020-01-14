package com.michael.project.dao.factory;

import com.michael.project.dao.ISessionAggStatDAO;
import com.michael.project.dao.ISessionDetailDAO;
import com.michael.project.dao.ISessionRandomExtractDAO;
import com.michael.project.dao.ITaskDAO;
import com.michael.project.dao.impl.SessionAggStatDAOImpl;
import com.michael.project.dao.impl.SessionDetailDAOImpl;
import com.michael.project.dao.impl.SessionRandomExtractDAOImpl;
import com.michael.project.dao.impl.TaskDAOImpl;

/**
 * DAO工厂类
 *
 * @author Michael Chu
 * @since 2020-01-08 15:13
 */
public class DAOFactory {

    private static final ITaskDAO TASK_DAO = new TaskDAOImpl();
    private static final ISessionAggStatDAO SESSION_AGG_STAT_DAO = new SessionAggStatDAOImpl();
    private static final ISessionRandomExtractDAO SESSION_RANDOM_DAO = new SessionRandomExtractDAOImpl();
    private static final ISessionDetailDAO SESSION_DETAIL_DAO = new SessionDetailDAOImpl();

    public static ITaskDAO getTaskDAO() {
        return TASK_DAO;
    }

    public static ISessionAggStatDAO getSessionAggStatDAO() {
        return SESSION_AGG_STAT_DAO;
    }

    public static ISessionRandomExtractDAO getSessionRandomDAO() {
        return SESSION_RANDOM_DAO;
    }

    public static ISessionDetailDAO getSessionDetailDAO() {
        return SESSION_DETAIL_DAO;
    }
}
