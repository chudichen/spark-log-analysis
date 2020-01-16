package com.michael.project.dao.factory;

import com.michael.project.dao.*;
import com.michael.project.dao.impl.*;

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
    private static final ITop10CategoryDAO TOP_10_CATEGORY_DAO = new Top10CategoryDAOImpl();

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

    public static ITop10CategoryDAO getTop10CategoryDAO() {
        return TOP_10_CATEGORY_DAO;
    }
}
