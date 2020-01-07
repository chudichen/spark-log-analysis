package com.michael.project.constant;

/**
 * 常量接口
 *
 * @author Michael Chu
 * @since 2020-01-06 16:19
 */
public interface Constants {

    /**
     * 项目配置相关的常量
     */
    String JDBC_DRIVER = "jdbc.driver";
    String JDBC_DATASOURCE_SIZE = "jdbc.datasource.size";
    String JDBC_URL = "jdbc.url";
    String JDBC_USER = "jdbc_user";
    String JDBC_PASSWORD = "jdbc.password";
    String JDBC_URL_PROD = "jdbc.url.prod";
    String JDBC_USER_PROD = "jdbc.user.prod";
    String JDBC_PASSWORD_PROD = "jdbc.password.prod";
    String SPARK_LOCAL = "spark.local";
    String SPARK_LOCAL_TASK_ID_SESSION = "spark.local.task.id.session";
    String SPARK_LOCAL_TASK_ID_PAGE = "spark.local.task.id.page";
    String SPARK_LOCAL_TASK_ID_PRODUCT = "spark.local.task.id.product";
    String KAFKA_METADATA_BROKER_LIST = "kafka.metadata.broker.list";
    String KAFKA_TOPICS = "kafka.topics";

    /**
     * Spark作业相关的常量
     */
    String SPARK_APP_NAME_SESSION = "UserVisitSessionAnalyzeSpark";
}
