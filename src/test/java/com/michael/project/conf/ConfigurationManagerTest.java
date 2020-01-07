package com.michael.project.conf;

import com.michael.project.constant.Constants;
import org.junit.Test;

/**
 * @author Michael Chu
 * @since 2020-01-06 16:10
 */
public class ConfigurationManagerTest {

    @Test
    public void loadTest() {
        String property = ConfigurationManager.getProperty(Constants.JDBC_DRIVER);
        System.out.println(property);
    }
}
