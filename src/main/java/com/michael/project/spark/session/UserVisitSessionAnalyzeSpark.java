package com.michael.project.spark.session;

import com.alibaba.fastjson.JSONObject;
import com.michael.project.conf.ConfigurationManager;
import com.michael.project.constant.Constants;
import com.michael.project.dao.ISessionAggStatDAO;
import com.michael.project.dao.ITaskDAO;
import com.michael.project.dao.factory.DAOFactory;
import com.michael.project.domain.SessionAggStat;
import com.michael.project.domain.Task;
import com.michael.project.mock.MockData;
import com.michael.project.util.DateUtils;
import com.michael.project.util.NumberUtils;
import com.michael.project.util.ParamUtils;
import com.michael.project.util.StringUtils;
import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;
import scala.Tuple2;

import java.util.Date;
import java.util.Iterator;
import java.util.Objects;


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

    /** 未找到匹配的category */
    private static final long NOT_FOUND = -1L;

    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName(Constants.SPARK_APP_NAME_SESSION)
                .setMaster("local[2]");

        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("INFO");
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
        JavaPairRDD<String, String> sessionId2AggInfoRDD = aggregateBySession(sqlContext, actionRDD);

//        sessionId2AggInfoRDD.foreach(stringStringTuple2 -> {
//            System.out.println(stringStringTuple2);
//        });
//        Tuple2<String, String> first = sessionId2AggInfoRDD.first();
//        System.out.println(first);
        // 接着，就要针对session粒度的聚合数据，按照使用者指定的筛选参数进行数据过滤
        // 相当于我们自己编写的算子，是要访问外面的任务参数对象的
        // 所以，大家集的我们之前说的，匿名内部类（算子函数），访问外部对象，是要给外部对象，是要给外部对象使用final修饰的
        // 重构，同时进行过滤和统计
        Accumulator<String> sessionAggStatAccumulator = sc.accumulator("", new SessionAggStatAccumulator());
        JavaPairRDD<String, String> filteredSessionId2AggInfoRDD = filterSessionAndAggStat(sessionId2AggInfoRDD, taskParam, sessionAggStatAccumulator);

        filteredSessionId2AggInfoRDD.count();
        // 计算出各个范围的session占比，并写入MySQL
        calculateAndPersistAggStat(sessionAggStatAccumulator.value(), task.getTaskId());

        sc.close();
    }

    /**
     * 过滤session数据，进行聚合统计
     *
     * @param sessionId2AggInfoRDD session
     * @param taskParam 参数
     * @param sessionAggStatAccumulator 累加器
     * @return
     */
    private static JavaPairRDD<String, String> filterSessionAndAggStat(JavaPairRDD<String, String> sessionId2AggInfoRDD, JSONObject taskParam, Accumulator<String> sessionAggStatAccumulator) {
        // 为了使用我们后面的ValidateUtils，所以，首先将所有的筛选参数拼接成一个连接串
        // 此外，这里其实大家不要觉得是多此一举
        // 其实我们是给后面的性能优化埋下粒一个伏笔
        String startAge = ParamUtils.getParam(taskParam, Constants.PARAM_START_AGE);
        String endAge = ParamUtils.getParam(taskParam, Constants.PARAM_END_AGE);
        String professionals = ParamUtils.getParam(taskParam, Constants.PARAM_PROFESSIONALS);
        String cities = ParamUtils.getParam(taskParam, Constants.PARAM_CITIES);
        String sex = ParamUtils.getParam(taskParam, Constants.PARAM_SEX);
        String keywords = ParamUtils.getParam(taskParam, Constants.PARAM_KEYWORDS);
        String categoryIds = ParamUtils.getParam(taskParam, Constants.PARAM_CATEGORY_IDS);

        String _parameter =
                (startAge == null ? Constants.STRING_EMPTY : Constants.PARAM_START_DATE + Constants.STRING_EQUAL + startAge + Constants.STRING_DELIMITER)
                        + (endAge == null ? Constants.STRING_EMPTY : Constants.PARAM_END_AGE + Constants.STRING_EQUAL + endAge + Constants.STRING_DELIMITER)
                        + (professionals == null ? Constants.STRING_EMPTY : Constants.PARAM_PROFESSIONALS + Constants.STRING_EQUAL + professionals + Constants.STRING_DELIMITER)
                        + (cities == null ? Constants.STRING_EMPTY : Constants.PARAM_CITIES + Constants.STRING_EQUAL + cities + Constants.STRING_DELIMITER)
                        + (sex == null ? Constants.STRING_EMPTY : Constants.PARAM_SEX + Constants.STRING_EQUAL + sex + Constants.STRING_DELIMITER)
                        + (keywords == null ? Constants.STRING_EMPTY : Constants.PARAM_KEYWORDS + Constants.STRING_EQUAL + keywords + Constants.STRING_DELIMITER)
                        + (categoryIds == null ? Constants.STRING_EMPTY : Constants.PARAM_CATEGORY_IDS + Constants.STRING_EQUAL + categoryIds + Constants.STRING_DELIMITER);

        if (_parameter.endsWith("\\|")) {
            _parameter = _parameter.substring(0, _parameter.length() - 1);
        }

        final String parameter = _parameter;
        // 根据筛选条件进行过滤
        return sessionId2AggInfoRDD.filter(tuple -> {
            // 首先，从tuple中，获取聚合数据
            String aggInfo = tuple._2;

//            // 接着，依次按照筛选条件进行过滤
//            // 按照年龄范围进行过滤（startAge，endAge）
//            if (!ValidUtils.between(aggInfo, Constants.FIELD_AGE, parameter, Constants.PARAM_START_AGE, Constants.PARAM_END_AGE)) {
//                return false;
//            }
//
//            // 按照职业范围进行过滤（professionals）
//            // 互联网，IT，软件
//            // 互联网
//            if (!ValidUtils.in(aggInfo, Constants.FIELD_PROFESSIONAL, parameter, Constants.PARAM_PROFESSIONALS)) {
//                return false;
//            }
//
//            // 按照城市范围进行过滤（cities）
//            // 北京，上海，广州，深圳
//            // 成都
//            if (!ValidUtils.in(aggInfo, Constants.FIELD_CITY, parameter, Constants.PARAM_CITIES)) {
//                return false;
//            }
//
//            // 按照性别进行过滤
//            // 男/女
//            // 男，女
//            if (!ValidUtils.equal(aggInfo, Constants.FIELD_SEX, parameter, Constants.PARAM_SEX)) {
//                return false;
//            }
//
//            /*
//             * 按照搜索词进行过滤
//             * 我们的session可能搜索了火锅，蛋糕，烧烤
//             * 我们的筛选条件可能是火锅，串串香，iphone手机
//             * 那么，in这个校验方法，主要判定session搜索的词中，有任何一个，与筛选条件中
//             * 任何一个搜索此相当，即通过
//             */
//            if (!ValidUtils.in(aggInfo, Constants.FIELD_SEARCH_KEYWORDS, parameter, Constants.PARAM_KEYWORDS)) {
//                return false;
//            }

            /*
             * 如果经过了之前的多个条件过滤之后,程序能够走到这里
             * 那么就说明，该session是通过粒用户指定的筛选条件的，也就是需要保留的session
             * 那么就要对session的访问时长和访问步长，进行统计，根据session对应的范围
             * 进行相应的累加计数
             */

            // 走到一步，那么就是需要计数的session
            sessionAggStatAccumulator.add(Constants.SESSION_COUNT);

            // 计算出session的访问时长和访问步长的范围，并进行相应的累加
            String visitLengthString = StringUtils.getFieldFromConcatString(aggInfo, "\\|", Constants.FIELD_VISIT_LENGTH);
            String stepLengthString = StringUtils.getFieldFromConcatString(aggInfo, "\\|", Constants.FIELD_STEP_LENGTH);
            if (StringUtils.isEmpty(visitLengthString) || StringUtils.isEmpty(stepLengthString)) {
                return false;
            }
            long visitLength = Long.parseLong(visitLengthString);
            long stepLength = Long.parseLong(stepLengthString);
            calculateVisitLength(visitLength, sessionAggStatAccumulator);
            calculateStepLength(stepLength, sessionAggStatAccumulator);
            return true;
        });
    }

    /**
     * 计算访问步长范围
     *
     * @param stepLength 步长
     * @param sessionAggStatAccumulator 累加器
     */
    private static void calculateStepLength(long stepLength, Accumulator<String> sessionAggStatAccumulator) {
        if(stepLength >= 1 && stepLength <= 3) {
            sessionAggStatAccumulator.add(Constants.STEP_PERIOD_1_3);
        } else if(stepLength >= 4 && stepLength <= 6) {
            sessionAggStatAccumulator.add(Constants.STEP_PERIOD_4_6);
        } else if(stepLength >= 7 && stepLength <= 9) {
            sessionAggStatAccumulator.add(Constants.STEP_PERIOD_7_9);
        } else if(stepLength >= 10 && stepLength <= 30) {
            sessionAggStatAccumulator.add(Constants.STEP_PERIOD_10_30);
        } else if(stepLength > 30 && stepLength <= 60) {
            sessionAggStatAccumulator.add(Constants.STEP_PERIOD_30_60);
        } else if(stepLength > 60) {
            sessionAggStatAccumulator.add(Constants.STEP_PERIOD_60);
        }
    }

    /**
     * 访问时长范围
     *
     * @param visitLength 访问时长
     */
    private static void calculateVisitLength(long visitLength, Accumulator<String> sessionAggStatAccumulator) {
        if(visitLength >=1 && visitLength <= 3) {
            sessionAggStatAccumulator.add(Constants.TIME_PERIOD_1s_3s);
        } else if(visitLength >=4 && visitLength <= 6) {
            sessionAggStatAccumulator.add(Constants.TIME_PERIOD_4s_6s);
        } else if(visitLength >=7 && visitLength <= 9) {
            sessionAggStatAccumulator.add(Constants.TIME_PERIOD_7s_9s);
        } else if(visitLength >=10 && visitLength <= 30) {
            sessionAggStatAccumulator.add(Constants.TIME_PERIOD_10s_30s);
        } else if(visitLength > 30 && visitLength <= 60) {
            sessionAggStatAccumulator.add(Constants.TIME_PERIOD_30s_60s);
        } else if(visitLength > 60 && visitLength <= 180) {
            sessionAggStatAccumulator.add(Constants.TIME_PERIOD_1m_3m);
        } else if(visitLength > 180 && visitLength <= 600) {
            sessionAggStatAccumulator.add(Constants.TIME_PERIOD_3m_10m);
        } else if(visitLength > 600 && visitLength <= 1800) {
            sessionAggStatAccumulator.add(Constants.TIME_PERIOD_10m_30m);
        } else if(visitLength > 1800) {
            sessionAggStatAccumulator.add(Constants.TIME_PERIOD_30m);
        }
    }

    /**
     * 对行为数据按session粒度进行聚合
     *
     * @param sqlContext {@link SQLContext} 上下文
     * @param actionRDD 行为数据RDD
     * @return session粒度聚合数据
     */
    private static JavaPairRDD<String, String> aggregateBySession(SQLContext sqlContext, JavaRDD<Row> actionRDD) {
        // 现在actionRDD中的元素是Row,一个Row就是一行用户访问行为记录，比如一次点击或者搜索
        // 我们现在需要将这个Row映射成<sessionId, Row>的格式
        JavaPairRDD<String, Row> sessionId2ActionRDD = actionRDD.mapToPair(row -> new Tuple2<>(row.getString(2), row));

        // 对行为数据按session粒度进行分组
        JavaPairRDD<String, Iterable<Row>> sessionId2ActionsRDD = sessionId2ActionRDD.groupByKey();

        JavaPairRDD<Long, String> userId2PartAggInfoRDD = sessionId2ActionsRDD.mapToPair(UserVisitSessionAnalyzeSpark::convert2UserIdAggTuple).filter(Objects::nonNull);

        // 查询所有用户数据，并映射成<userId, Row>的格式
        String sql = "select * from user_info";
        JavaRDD<Row> userInfoRDD = sqlContext.sql(sql).javaRDD();
        JavaPairRDD<Long, Row> userId2InfoRDD = userInfoRDD.mapToPair(row -> new Tuple2<>(row.getLong(0), row));

        // 将session粒度聚合数据，与用户信息进行join
        JavaPairRDD<Long, Tuple2<String, Row>> userId2FullInfoRDD = userId2PartAggInfoRDD.join(userId2InfoRDD);

        // 对join起来的数据进行拼接，并且返回<sessionId, fullAggInfo>格式的数据
        return userId2FullInfoRDD.mapToPair(tuple -> {
                String partAggInfo = tuple._2._1;
                Row userInfoRow = tuple._2._2;

                String sessionId = StringUtils.getFieldFromConcatString(
                        partAggInfo, "\\|", Constants.FIELD_SESSION_ID);

                int age = userInfoRow.getInt(3);
                String professional = userInfoRow.getString(4);
                String city = userInfoRow.getString(5);
                String sex = userInfoRow.getString(6);

                String fullAggInfo = partAggInfo + "|"
                        + Constants.FIELD_AGE + "=" + age + "|"
                        + Constants.FIELD_PROFESSIONAL + "=" + professional + "|"
                        + Constants.FIELD_CITY + "=" + city + "|"
                        + Constants.FIELD_SEX + "=" + sex;

                return new Tuple2<>(sessionId, fullAggInfo);
        });
    }

    private static Tuple2<Long, String> convert2UserIdAggTuple(Tuple2<String, Iterable<Row>> tuple) {
        try {
            String sessionId = tuple._1;
            Iterator<Row> iterator = tuple._2.iterator();

            StringBuilder searchKeywordsBuilder = new StringBuilder();
            StringBuilder clickCategoryIdsBuilder = new StringBuilder();

            // session的起始时间和结束时间
            Date startTime = null;
            Date endTime = null;
            // session的访问步长
            int stepLength = 0;

            long userId = NOT_FOUND;
            // 遍历session所有的访问行为
            while (iterator.hasNext()) {
                Row row = iterator.next();
                // 提取每个访问行为的搜索词字段和点击品类字段
                if (userId == NOT_FOUND) {
                    userId = row.getLong(1);
                }

                String searchKeyword = row.getString(5);
                long clickCategoryId;
                try {
                    clickCategoryId = row.getLong(6);
                } catch (NullPointerException e) {
                    clickCategoryId = NOT_FOUND;
                }

                // 实际上这里要对数据说明一下
                // 并不是每一行访问行为都有searchKeyword和clickCategoryId两个字段
                // 其实，只有搜索行为，是有searchKeyword字段的
                // 只有点击品类的行为，是有clickCategoryId字段的
                // 所以，任何一行行为数据，都不可能两个字段都有，所以数据是可能出现null值的
                // 我们决定是否将搜索词或点击品类id拼接到字符串中去
                // 首先要满足：不能是null值
                // 其次，之前的字符串中还没有搜索词或者点击品类id
                if (StringUtils.isNotEmpty(searchKeyword)) {
                    if (!searchKeywordsBuilder.toString().contains(searchKeyword)) {
                        searchKeywordsBuilder.append(searchKeyword).append(",");
                    }
                }

                if (clickCategoryId != NOT_FOUND && !clickCategoryIdsBuilder.toString().contains(String.valueOf(clickCategoryId))) {
                    clickCategoryIdsBuilder.append(clickCategoryId).append(",");
                }

                // 计算session开始和结束时间
                Date actionTime = DateUtils.parseTime(row.getString(4));
                if (actionTime == null) {
                    continue;
                }

                if (startTime == null) {
                    startTime = actionTime;
                }
                if (endTime == null) {
                    endTime = actionTime;
                }

                if(actionTime.before(startTime)) {
                    startTime = actionTime;
                }
                if(actionTime.after(endTime)) {
                    endTime = actionTime;
                }

                // 计算访问步长
                stepLength++;
            }

            String searchKeywords = StringUtils.trimComma(searchKeywordsBuilder.toString());
            String clickCategoryIds = StringUtils.trimComma(clickCategoryIdsBuilder.toString());

            // 计算session访问时长(秒)
            if (startTime == null) {
                return null;
            }
            long visitLength = (endTime.getTime() - startTime.getTime()) / 1000;

            String partAggInfo = Constants.FIELD_SESSION_ID + "=" + sessionId + "|"
                    + Constants.FIELD_SEARCH_KEYWORDS + "=" + searchKeywords + "|"
                    + Constants.FIELD_CLICK_CATEGORY_IDS + "=" + clickCategoryIds + "|"
                    + Constants.FIELD_VISIT_LENGTH + "=" + visitLength + "|"
                    + Constants.FIELD_STEP_LENGTH + "=" + stepLength;

            return new Tuple2<>(userId, partAggInfo);
        } catch (Exception e) {
            return null;
        }
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

    /**
     * 计算各session范围占比，并写入MySQL
     *
     * @param value 拼接值
     * @param taskId 任务ID
     */
    private static void calculateAndPersistAggStat(String value, Long taskId) {
        // 从Accumulator统计串中获取值
        long session_count = Long.parseLong(Objects.requireNonNull(StringUtils.getFieldFromConcatString(value, "\\|", Constants.SESSION_COUNT)));

        long visit_length_1s_3s = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.TIME_PERIOD_1s_3s));
        long visit_length_4s_6s = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.TIME_PERIOD_4s_6s));
        long visit_length_7s_9s = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.TIME_PERIOD_7s_9s));
        long visit_length_10s_30s = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.TIME_PERIOD_10s_30s));
        long visit_length_30s_60s = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.TIME_PERIOD_30s_60s));
        long visit_length_1m_3m = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.TIME_PERIOD_1m_3m));
        long visit_length_3m_10m = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.TIME_PERIOD_3m_10m));
        long visit_length_10m_30m = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.TIME_PERIOD_10m_30m));
        long visit_length_30m = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.TIME_PERIOD_30m));

        long step_length_1_3 = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.STEP_PERIOD_1_3));
        long step_length_4_6 = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.STEP_PERIOD_4_6));
        long step_length_7_9 = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.STEP_PERIOD_7_9));
        long step_length_10_30 = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.STEP_PERIOD_10_30));
        long step_length_30_60 = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.STEP_PERIOD_30_60));
        long step_length_60 = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.STEP_PERIOD_60));

        // 计算各个访问时长和访问步长的范围
        double visit_length_1s_3s_ratio = NumberUtils.formatDouble(
                visit_length_1s_3s / session_count, 2);
        double visit_length_4s_6s_ratio = NumberUtils.formatDouble(
                visit_length_4s_6s / session_count, 2);
        double visit_length_7s_9s_ratio = NumberUtils.formatDouble(
                visit_length_7s_9s / session_count, 2);
        double visit_length_10s_30s_ratio = NumberUtils.formatDouble(
                visit_length_10s_30s / session_count, 2);
        double visit_length_30s_60s_ratio = NumberUtils.formatDouble(
                visit_length_30s_60s / session_count, 2);
        double visit_length_1m_3m_ratio = NumberUtils.formatDouble(
                visit_length_1m_3m / session_count, 2);
        double visit_length_3m_10m_ratio = NumberUtils.formatDouble(
                visit_length_3m_10m / session_count, 2);
        double visit_length_10m_30m_ratio = NumberUtils.formatDouble(
                visit_length_10m_30m / session_count, 2);
        double visit_length_30m_ratio = NumberUtils.formatDouble(
                visit_length_30m / session_count, 2);

        double step_length_1_3_ratio = NumberUtils.formatDouble(
                step_length_1_3 / session_count, 2);
        double step_length_4_6_ratio = NumberUtils.formatDouble(
                step_length_4_6 / session_count, 2);
        double step_length_7_9_ratio = NumberUtils.formatDouble(
                step_length_7_9 / session_count, 2);
        double step_length_10_30_ratio = NumberUtils.formatDouble(
                step_length_10_30 / session_count, 2);
        double step_length_30_60_ratio = NumberUtils.formatDouble(
                step_length_30_60 / session_count, 2);
        double step_length_60_ratio = NumberUtils.formatDouble(
                step_length_60 / session_count, 2);

        // 将统计结果封装为Domain对象
        SessionAggStat sessionAggStat = new SessionAggStat();
        sessionAggStat.setTaskId(taskId);
        sessionAggStat.setSession_count(session_count);
        sessionAggStat.setVisit_length_1s_3s_ratio(visit_length_1s_3s_ratio);
        sessionAggStat.setVisit_length_4s_6s_ratio(visit_length_4s_6s_ratio);
        sessionAggStat.setVisit_length_7s_9s_ratio(visit_length_7s_9s_ratio);
        sessionAggStat.setVisit_length_10s_30s_ratio(visit_length_10s_30s_ratio);
        sessionAggStat.setVisit_length_30s_60s_ratio(visit_length_30s_60s_ratio);
        sessionAggStat.setVisit_length_1m_3m_ratio(visit_length_1m_3m_ratio);
        sessionAggStat.setVisit_length_3m_10m_ratio(visit_length_3m_10m_ratio);
        sessionAggStat.setVisit_length_10m_30m_ratio(visit_length_10m_30m_ratio);
        sessionAggStat.setVisit_length_30m_ratio(visit_length_30m_ratio);
        sessionAggStat.setStep_length_1_3_ratio(step_length_1_3_ratio);
        sessionAggStat.setStep_length_4_6_ratio(step_length_4_6_ratio);
        sessionAggStat.setStep_length_7_9_ratio(step_length_7_9_ratio);
        sessionAggStat.setStep_length_10_30_ratio(step_length_10_30_ratio);
        sessionAggStat.setStep_length_30_60_ratio(step_length_30_60_ratio);
        sessionAggStat.setStep_length_60_ratio(step_length_60_ratio);

        // 调用对应的DAO插入统计结果
        ISessionAggStatDAO sessionAggStatDAO = DAOFactory.getSessionAggStatDAO();
        sessionAggStatDAO.insert(sessionAggStat);
    }

}
