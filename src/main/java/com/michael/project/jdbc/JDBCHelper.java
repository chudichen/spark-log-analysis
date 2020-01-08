package com.michael.project.jdbc;

import com.michael.project.conf.ConfigurationManager;
import com.michael.project.constant.Constants;

import javax.xml.transform.Result;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.LinkedList;
import java.util.List;

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

    /**
     * 第三步：实现单例的过程中，创建唯一的数据库连接池
     *
     * 私有化构造方法
     *
     * JDBCHelper在整个程序运行生命周期中，只会创建一次实例
     * 在这一次创建实例的过程中，就会调用JDBCHelper()构造方法
     * 此时，就可以在构造方法中，去创建自己唯一的一个数据库连接池
     */
    private JDBCHelper() {
        // 首先第一步，获取数据库连接池的大小，就是说，数据库连接池中要放多少个数据库连接
        // 这个，可以通过在配置文件中配置的方式，来灵活的设定
        Integer datasourceSize = ConfigurationManager.getInteger(Constants.JDBC_DATASOURCE_SIZE);

        for (int i = 0; i < datasourceSize; i++) {
            boolean local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);
            String url;
            String user;
            String password;
            if (local) {
                url = ConfigurationManager.getProperty(Constants.JDBC_URL);
                user = ConfigurationManager.getProperty(Constants.JDBC_USER);
                password = ConfigurationManager.getProperty(Constants.JDBC_PASSWORD);
            } else {
                url = ConfigurationManager.getProperty(Constants.JDBC_URL_PROD);
                user = ConfigurationManager.getProperty(Constants.JDBC_USER_PROD);
                password = ConfigurationManager.getProperty(Constants.JDBC_PASSWORD_PROD);
            }

            try {
                Connection connection = DriverManager.getConnection(url, user, password);
                datasource.push(connection);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 第四步，提供获取数据库连接的方法
     * 有可能，你去获取的时候，这个时候，连接都被用光了，你暂时获取不到数据库连接
     * 所以我们要自己编码实现一个简单的等待机制，去等待获取数据库连接
     *
     * @return 数据库连接
     */
    public synchronized Connection getConnection() {
        while (datasource.size() == 0) {
            try {
                Thread.sleep(10);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return datasource.poll();
    }

    /**
     * 执行增删该SQL语句
     *
     * @param sql 执行sql
     * @param params 参数
     * @return 影响行数
     */
    public int executeUpdate(String sql, Object[] params) {
        int row = 0;
        Connection conn = null;
        PreparedStatement preparedStatement = null;

        try {
            conn = getConnection();
            conn.setAutoCommit(false);

            preparedStatement = conn.prepareStatement(sql);

            if (params != null && params.length > 0) {
                for (int i = 0; i < params.length; i++) {
                    preparedStatement.setObject(i + 1, params[i]);
                }
            }

            row = preparedStatement.executeUpdate();
            conn.commit();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (conn != null) {
                datasource.push(conn);
            }
        }
        return row;
    }

    /**
     * 执行查询SQL语句
     *
     * @param sql SQL语句
     * @param params 参数
     * @param callback 回调
     */
    public void executeQuery(String sql, Object[] params, QueryCallback callback) {
        Connection conn = null;
        PreparedStatement preparedStatement;
        ResultSet rs;

        try {
            conn = getConnection();
            preparedStatement = conn.prepareStatement(sql);

            if (params != null && params.length > 0) {
                for (int i = 0; i < params.length; i++) {
                    preparedStatement.setObject(i + 1, params[i]);
                }
            }

            rs = preparedStatement.executeQuery();
            callback.process(rs);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (conn != null) {
                datasource.push(conn);
            }
        }
    }

    public int[] executeBath(String sql, List<Object[]> paramsList) {
        int[] row = null;
        Connection conn = null;
        PreparedStatement preparedStatement;

        try {
            conn = getConnection();

            // 第一步：使用Connection对象，取消自动提交
            conn.setAutoCommit(false);
            preparedStatement = conn.prepareStatement(sql);

            // 第二步：使用PreparedStatement.addBatch()方法加入批量的SQL参数
            if (paramsList != null && paramsList.size() > 0) {
                for (Object[] params : paramsList) {
                    for (int i = 0; i < params.length; i++) {
                        preparedStatement.setObject(i + 1, params[i]);
                    }
                    preparedStatement.addBatch();
                }
            }

            // 第三步：使用PreparedStatement.executeBatch()方法，执行批量的SQL语句
            row = preparedStatement.executeBatch();
            conn.commit();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (conn != null) {
                datasource.push(conn);
            }
        }
        return row;
    }

    /**
     * 静态内部类：查询回调接口
     */
    public static interface QueryCallback {

        /**
         * 处理查询结果
         *
         * @param rs 结果
         * @throws Exception 异常
         */
        void process(ResultSet rs) throws Exception;
    }
}
