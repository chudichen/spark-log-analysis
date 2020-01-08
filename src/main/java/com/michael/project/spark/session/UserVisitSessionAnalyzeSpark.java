package com.michael.project.spark.session;

import com.alibaba.fastjson.JSONObject;
import com.michael.project.conf.ConfigurationManager;
import com.michael.project.constant.Constants;
import com.michael.project.dao.ITaskDAO;
import com.michael.project.dao.factory.DAOFactory;
import com.michael.project.domain.Task;
import com.michael.project.mock.MockData;
import com.michael.project.util.ParamUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;
import scala.Tuple2;


/**
 * 用户访问session分析Spark作业
 *
 * 接收用户创建的分析任务，用户可能指定的条件如下：
 *
 * 1. 时间范围：起始时间～结束时间
 * 2. 性别：男或女
 * 3. 年龄范围
 * 4. 职业：多选
 * 5. 城市：多选
 * 6. 搜索词：多个搜索词，只要某个session中的任何一个action搜索过制定的关键词，那么session就符合条件
 * 7. 点击品类：多个品类，只要某个session中的任何一个action点击过某个品类，那么session就符合条件
 *
 * 我们的Spark作业如何接受用户创建的任务？
 *
 * J2EE平台在接收用户创建任务的请求之后，会将任务信息插入MySQL的task表中，任务参数以JSON格式封装在task_param字段中
 *
 * 接着J2EE平台会执行我们的spark-submit shell脚本，并将taskId作为参数传递给spark-submit shell脚本
 * spark-submit shell脚本，在执行时，是可以接收参数的，并且会将接收的参数，传递给Spark作业的main函数
 * 参数就封装在main汉书的args数组中
 *
 * 这是spark本身提供的特性
 *
 * @author Michael Chu
 * @since 2020-01-07 08:39
 */
public class UserVisitSessionAnalyzeSpark {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName(Constants.SPARK_APP_NAME_SESSION)
                .setMaster("local[2]");

        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = getSQLContext(sc.sc());

        // 生成模拟测试数据
        mockData(sc, sqlContext);

        // 创建需要使用的DAO组件
        ITaskDAO taskDAO = DAOFactory.getTaskDAO();

        // 首先得查询出来指定的任务，并获取任务的查询参数
        Long taskId = ParamUtils.getTaskIdFromArgs(args, Constants.SPARK_LOCAL_TASK_ID_SESSION);
        if (taskId == null) {
            return;
        }
        Task task = taskDAO.findById(taskId);
        JSONObject taskParam = JSONObject.parseObject(task.getTaskParam());

        // 如果要进行session粒度的数据聚合
        // 首先要从user_visit_action表中，查询出来指定日期范围内的行为数据
        JavaRDD<Row> actionRDD = getActionRDDByDateRange(sqlContext, taskParam);

        // 首先，可以将行为数据，按照session_id进行groupByKey分组
        // 此时的数据的粒度就是session粒度粒，然后呢，可以将session粒度的数据
        // 与用户信息数据，进行join
        // 然后就可以获取到session粒度的数据，同时呢，数据粒面还包含粒session对应的user的信息
        JavaPairRDD<String, String> stringStringJavaPairRDD = aggregateBySession(sqlContext, actionRDD);

        sc.close();
    }

    /**
     * 对行为数据按session粒度进行聚合
     *
     * @param sqlContext {@link SQLContext} 上下文
     * @param actionRDD 行为数据RDD
     * @return session粒度聚合数据
     */
    private static JavaPairRDD<String, String> aggregateBySession(SQLContext sqlContext, JavaRDD<Row> actionRDD) {
        JavaPairRDD<String, Row> sessionId2ActionRDD = actionRDD.mapToPair(
                /*
                 * PairFunction
                 * 第一个参数，相当于是函数的输入
                 * 第二个参数和第三个参数，相当于是函数的输出（Tuple），分别是Tuple第一个和第二个值
                 */
                new PairFunction<Row, String, Row>() {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public Tuple2<String, Row> call(Row row) {
                        return new Tuple2<>(row.getString(2), row);
                    }
                });
        return null;
    }

    /**
     * 获取SQLContext，
     * 如果是在本地测试环境的话，那么就生成SQLContext对象
     * 如果是在生产环境的话，那么就生成HiveContext对象
     *
     * @param sc {@link SparkContext}
     * @return {@link SQLContext}
     */
    private static SQLContext getSQLContext(SparkContext sc) {
        boolean local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);
        if (local) {
            return new SQLContext(sc);
        } else {
            return new HiveContext(sc);
        }
    }

    /**
     * 生成模拟数据（只有本地模式，才会去生成测试数据）
     *
     * @param sc {@link JavaSparkContext}
     * @param sqlContext {@link SQLContext}
     */
    private static void mockData(JavaSparkContext sc, SQLContext sqlContext) {
        boolean local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);
        if (local) {
            MockData.mock(sc, sqlContext);
        }
    }

    /**
     * 获取指定日期范围内的用户访问行为数据
     *
     * @param sqlContext {@link SQLContext}
     * @param taskParam {@link JSONObject} 任务数据
     * @return 行为数据RDD
     */
    private static JavaRDD<Row> getActionRDDByDateRange(SQLContext sqlContext, JSONObject taskParam) {
        String startDate = ParamUtils.getParam(taskParam, Constants.PARAM_START_DATE);
        String endDate = ParamUtils.getParam(taskParam, Constants.PARAM_END_DATE);

        String sql =
                "select * " +
                "from user_visit_action where date >= '" + startDate + "' " +
                "and date <= '" + endDate + "'";
        Dataset<Row> actionDF = sqlContext.sql(sql);
        return actionDF.javaRDD();
    }

}
