package com.michael.project.util;

import com.michael.project.conf.ConfigurationManager;
import com.michael.project.constant.Constants;
import com.michael.project.mock.MockData;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;

/**
 * Spark工具类
 *
 * @author Michael Chu
 * @since 2020-01-06 16:50
 */
public class SparkUtils {

    /**
     * 根据当前是否本地测试的配置决定，如何设置SparkConf的Master
     *
     * @param conf 配置
     */
    public static void setMaster(SparkConf conf) {
        boolean local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);
        if (local) {
            conf.setMaster("local");
        }
    }

    /**
     * 获取SQLContext,如果spark.local设置为true，那么就创建SQLContext：否则创建HiveContext
     *
     * @param sc spark上下文
     * @return {@link SQLContext}
     */
    public static SQLContext getSQLContext(SparkContext sc) {
        Boolean local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);
        if (local) {
            return new SQLContext(sc);
        } else {
            return new HiveContext(sc);
        }
    }

    /**
     * 生成模拟数据，如果spark.local配置设置为{@code true}则生成模拟数据，否则不生成
     *
     * @param sc {@link JavaSparkContext}
     * @param sqlContext {@link SQLContext}
     */
    public static void mockData(JavaSparkContext sc, SQLContext sqlContext) {
        boolean local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);
        if (local) {
            MockData.mock(sc, sqlContext);
        }
    }
}
