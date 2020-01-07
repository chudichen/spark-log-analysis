package com.michael.project.jdbc;

import com.michael.project.conf.ConfigurationManager;
import com.michael.project.constant.Constants;

import java.sql.Connection;
import java.util.LinkedList;

/**
 * JDBC辅助组件
 *
 * 在正式的项目的代码编写过程中，是完全严格按照大公司的coding标准来的
 * 也就是说，在代码中，是不能出现hard code（硬编码）的字符，
 * 比如“Tom”，“com.mysql.cj.jdbc.Driver”
 * 所有这些东西，都需要通过常量来封装和使用
 *
 * @author Michael Chu
 * @since 2020-01-07 19:49
 */
public class JDBCHelper {

    /*
     * 第一步：在静态代码块中，直接加载数据库的驱动
     */
    static {
        try {
            String driver = ConfigurationManager.getProperty(Constants.JDBC_DRIVER);
            Class.forName(driver);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static JDBCHelper instance = null;

    /**
     * 获取单例
     *
     * @return 单例
     */
    public static JDBCHelper getInstance() {
        if (instance == null) {
            synchronized (JDBCHelper.class) {
                if (instance == null) {
                    instance = new JDBCHelper();
                }
            }
        }
        return instance;
    }

    /** 数据库连接池 */
    private LinkedList<Connection> datasource = new LinkedList<>();
}
