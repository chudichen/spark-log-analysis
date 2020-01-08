package com.michael.project.dao.factory;

import com.michael.project.dao.ITaskDAO;
import com.michael.project.dao.impl.TaskDAOImpl;

/**
 * DAO工厂类
 *
 * @author Michael Chu
 * @since 2020-01-08 15:13
 */
public class DAOFactory {

    private static final ITaskDAO TASK_DAO = new TaskDAOImpl();

    public static ITaskDAO getTaskDAO() {
        return TASK_DAO;
    }
}
