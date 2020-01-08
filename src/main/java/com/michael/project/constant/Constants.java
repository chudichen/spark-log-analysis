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
    String JDBC_USER = "jdbc.user";
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
    String SPARK_APP_NAME_PAGE = "PageOneStepConvertRateSpark";
    String FIELD_SESSION_ID = "sessionId";
    String FIELD_SEARCH_KEYWORDS = "searchKeywords";
    String FIELD_CLICK_CATEGORY_IDS = "clickCategoryIds";
    String FIELD_AGE = "age";
    String FIELD_PROFESSIONAL = "professional";
    String FIELD_CITY = "city";
    String FIELD_SEX = "sex";
    String FIELD_VISIT_LENGTH = "visitLength";
    String FIELD_STEP_LENGTH = "stepLength";
    String FIELD_START_TIME = "startTime";
    String FIELD_CLICK_COUNT = "clickCount";
    String FIELD_ORDER_COUNT = "orderCount";
    String FIELD_PAY_COUNT = "payCount";
    String FIELD_CATEGORY_ID = "categoryid";

    /**
     * 任务相关的常量
     */
    String PARAM_START_DATE = "startDate";
    String PARAM_END_DATE = "endDate";
}
