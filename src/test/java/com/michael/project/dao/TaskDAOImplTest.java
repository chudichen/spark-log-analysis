package com.michael.project.dao;

import com.michael.project.dao.factory.DAOFactory;
import com.michael.project.domain.Task;
import org.junit.Test;

/**
 * @author Michael Chu
 * @since 2020-01-08 15:56
 */
public class TaskDAOImplTest {

    private ITaskDAO taskDAO = DAOFactory.getTaskDAO();

    @Test
    public void findByIdTest() {
        Task task = taskDAO.findById(2);
        System.out.println(task);
    }
}
