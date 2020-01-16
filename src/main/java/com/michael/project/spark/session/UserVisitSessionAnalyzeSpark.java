package com.michael.project.spark.session;

import com.alibaba.fastjson.JSONObject;
import com.michael.project.conf.ConfigurationManager;
import com.michael.project.constant.Constants;
import com.michael.project.dao.*;
import com.michael.project.dao.factory.DAOFactory;
import com.michael.project.domain.*;
import com.michael.project.mock.MockData;
import com.michael.project.util.DateUtils;
import com.michael.project.util.NumberUtils;
import com.michael.project.util.ParamUtils;
import com.michael.project.util.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.util.AccumulatorV2;
import scala.Tuple2;

import java.util.*;


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
    private static final AccumulatorV2<String, String> SESSION_AGG_STAT_ACCUMULATOR = new SessionAggStatAccumulator();
    private static ISessionDetailDAO sessionDetailDAO = DAOFactory.getSessionDetailDAO();

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
        JavaPairRDD<String, Row> sessionId2actionRDD = getSessionId2ActionRDD(actionRDD);

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

        sc.sc().register(SESSION_AGG_STAT_ACCUMULATOR);
        JavaPairRDD<String, String> filteredSessionId2AggInfoRDD = filterSessionAndAggStat(sessionId2AggInfoRDD, taskParam);

        /*
         *  对于Accumulator这种分布式累加计算的变量的使用，有一个重要说明
         *  从Accumulator中，获取值再写入数据库的时候，一定要在某个action操作之后
         *  如果没有action的话，那么整个程序根本不会运行，必须把能够出发job执行的操作，
         *  放在最终写入MySQL方法之前，计算出来的结果，在J2EE中，是怎么显示的，使用柱状图显示
         */
//        System.out.println(filteredSessionId2AggInfoRDD.count());

        randomExtractSession(taskId, filteredSessionId2AggInfoRDD, sessionId2actionRDD);

        /*
         * 特别说明
         * 我们知道，要将上一个功能的session聚合统计数据获取到，就必须要是在一个action操作触发job之后
         * 才能从Accumulator中获取到数据，否则是获取不到数据的，因为没有job执行，Accumulator的值为空
         * 所以，我们在这里，将随机抽取的功能的实现代码，放在session聚合统计功能的最终计算和写库之前因为
         * 随机抽取功能中，有一个countByKey算子，是action操作，会触发job
         */

        // 计算出各个范围的session占比，并写入MySQL
        calculateAndPersistAggStat(SESSION_AGG_STAT_ACCUMULATOR.value(), task.getTaskId());

        /*
         * session聚合统计（统计出访问时长和访问步长，各个区间的session数量占用session数量的比例）
         *
         * 如果不进行重构，直接来实现，思路：
         * 1. actionRDD，映射成<sessionId, Row>的格式
         * 2. 按sessionId聚合，计算出每个session的访问时长和访问步长，生成一个新的RDD
         * 3. 遍历新生成的RDD，将每个session的访问时长和访问步长，去更新自定义Accumulator中的对应的值
         * 4. 使用自定义Accumulator中的统计值，去计算各个区间的比例
         * 5. 将最后计算出来的结果，写入Mysql对应的表中
         *
         * 普通实现思路的问题：
         * 1. 为什么还要用actionRDD，去映射？其实我们之前在session聚合的时候，映射已经做过了，多此一举。
         * 2. 是不是一定要，为了session聚合这个功能，单独去遍历一遍session？起始没有必要，已经有session数据
         * 之前过滤session的时候，起始就相当于，是在遍历session，那么就没有必要再遍历一遍粒。
         *
         * 重构实现思路：
         * 1. 不要去生成任何新的RDD（处理上亿的数据）
         * 2. 不要去单独遍历一遍session的数据（处理上千万的数据）
         * 3. 可以在进行session聚合的时候，就直接计算出每个session的访问时长和访问步长
         * 4. 在进行过滤的时候，本来就要遍历所有的聚合session信息，此时，就可以在某个session通过筛选条件后将
         * 其访问时长和访问步长，累加到自定义的Accumulator上面去
         * 5. 就是两种截然不同的思考方式和实现方式，在面对上亿，上千万数据的时候，甚至可以节省时间长达半个小时，
         * 或者数个小时
         *
         */

        getTop10Category(taskId, filteredSessionId2AggInfoRDD, sessionId2actionRDD);
        sc.close();
    }

    /**
     * 获取sessionId到访问行为数据的映射的RDD
     *
     * @param actionRDD RDD
     * @return 行为映射
     */
    private static JavaPairRDD<String, Row> getSessionId2ActionRDD(JavaRDD<Row> actionRDD) {
        return actionRDD.mapToPair(row -> new Tuple2<>(row.getString(2), row));
    }

    /**
     * 过滤session数据，进行聚合统计
     *
     * @param sessionId2AggInfoRDD session
     * @param taskParam 参数
     * @return 过滤后的RDD
     */
    private static JavaPairRDD<String, String> filterSessionAndAggStat(JavaPairRDD<String, String> sessionId2AggInfoRDD, JSONObject taskParam) {
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
            SESSION_AGG_STAT_ACCUMULATOR.add(Constants.SESSION_COUNT);

            // 计算出session的访问时长和访问步长的范围，并进行相应的累加
            String visitLengthString = StringUtils.getFieldFromConcatString(aggInfo, "\\|", Constants.FIELD_VISIT_LENGTH);
            String stepLengthString = StringUtils.getFieldFromConcatString(aggInfo, "\\|", Constants.FIELD_STEP_LENGTH);
            if (StringUtils.isEmpty(visitLengthString) || StringUtils.isEmpty(stepLengthString)) {
                return false;
            }
            long visitLength = Long.parseLong(visitLengthString);
            long stepLength = Long.parseLong(stepLengthString);
            // 计算访问时间长
            if(visitLength >=1 && visitLength <= 3) {
                SESSION_AGG_STAT_ACCUMULATOR.add(Constants.TIME_PERIOD_1s_3s);
            } else if(visitLength >=4 && visitLength <= 6) {
                SESSION_AGG_STAT_ACCUMULATOR.add(Constants.TIME_PERIOD_4s_6s);
            } else if(visitLength >=7 && visitLength <= 9) {
                SESSION_AGG_STAT_ACCUMULATOR.add(Constants.TIME_PERIOD_7s_9s);
            } else if(visitLength >=10 && visitLength <= 30) {
                SESSION_AGG_STAT_ACCUMULATOR.add(Constants.TIME_PERIOD_10s_30s);
            } else if(visitLength > 30 && visitLength <= 60) {
                SESSION_AGG_STAT_ACCUMULATOR.add(Constants.TIME_PERIOD_30s_60s);
            } else if(visitLength > 60 && visitLength <= 180) {
                SESSION_AGG_STAT_ACCUMULATOR.add(Constants.TIME_PERIOD_1m_3m);
            } else if(visitLength > 180 && visitLength <= 600) {
                SESSION_AGG_STAT_ACCUMULATOR.add(Constants.TIME_PERIOD_3m_10m);
            } else if(visitLength > 600 && visitLength <= 1800) {
                SESSION_AGG_STAT_ACCUMULATOR.add(Constants.TIME_PERIOD_10m_30m);
            } else if(visitLength > 1800) {
                SESSION_AGG_STAT_ACCUMULATOR.add(Constants.TIME_PERIOD_30m);
            }

            // 计算访问步长
            if(stepLength >= 1 && stepLength <= 3) {
                SESSION_AGG_STAT_ACCUMULATOR.add(Constants.STEP_PERIOD_1_3);
            } else if(stepLength >= 4 && stepLength <= 6) {
                SESSION_AGG_STAT_ACCUMULATOR.add(Constants.STEP_PERIOD_4_6);
            } else if(stepLength >= 7 && stepLength <= 9) {
                SESSION_AGG_STAT_ACCUMULATOR.add(Constants.STEP_PERIOD_7_9);
            } else if(stepLength >= 10 && stepLength <= 30) {
                SESSION_AGG_STAT_ACCUMULATOR.add(Constants.STEP_PERIOD_10_30);
            } else if(stepLength > 30 && stepLength <= 60) {
                SESSION_AGG_STAT_ACCUMULATOR.add(Constants.STEP_PERIOD_30_60);
            } else if(stepLength > 60) {
                SESSION_AGG_STAT_ACCUMULATOR.add(Constants.STEP_PERIOD_60);
            }
            return true;
        });
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

                if(startTime == null) {
                    startTime = actionTime;
                }
                if(endTime == null) {
                    endTime = actionTime;
                }

                if (actionTime == null) {
                    return null;
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
//            Random random = new Random();
//            long visitLength = random.nextInt(1800);

            String partAggInfo = Constants.FIELD_SESSION_ID + "=" + sessionId + "|"
                    + Constants.FIELD_SEARCH_KEYWORDS + "=" + searchKeywords + "|"
                    + Constants.FIELD_CLICK_CATEGORY_IDS + "=" + clickCategoryIds + "|"
                    + Constants.FIELD_VISIT_LENGTH + "=" + visitLength + "|"
                    + Constants.FIELD_STEP_LENGTH + "=" + stepLength + "|"
                    + Constants.FIELD_START_TIME + "=" + DateUtils.formatTime(startTime);

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
                (double)visit_length_1s_3s / (double)session_count, 2);
        double visit_length_4s_6s_ratio = NumberUtils.formatDouble(
                (double)visit_length_4s_6s / (double)session_count, 2);
        double visit_length_7s_9s_ratio = NumberUtils.formatDouble(
                (double)visit_length_7s_9s / (double)session_count, 2);
        double visit_length_10s_30s_ratio = NumberUtils.formatDouble(
                (double)visit_length_10s_30s / (double)session_count, 2);
        double visit_length_30s_60s_ratio = NumberUtils.formatDouble(
                (double)visit_length_30s_60s / (double)session_count, 2);
        double visit_length_1m_3m_ratio = NumberUtils.formatDouble(
                (double)visit_length_1m_3m / (double)session_count, 2);
        double visit_length_3m_10m_ratio = NumberUtils.formatDouble(
                (double)visit_length_3m_10m / (double)session_count, 2);
        double visit_length_10m_30m_ratio = NumberUtils.formatDouble(
                (double)visit_length_10m_30m / (double)session_count, 2);
        double visit_length_30m_ratio = NumberUtils.formatDouble(
                (double)visit_length_30m / (double)session_count, 2);

        double step_length_1_3_ratio = NumberUtils.formatDouble(
                (double)step_length_1_3 / (double)session_count, 2);
        double step_length_4_6_ratio = NumberUtils.formatDouble(
                (double)step_length_4_6 / (double)session_count, 2);
        double step_length_7_9_ratio = NumberUtils.formatDouble(
                (double)step_length_7_9 / (double)session_count, 2);
        double step_length_10_30_ratio = NumberUtils.formatDouble(
                (double)step_length_10_30 / (double)session_count, 2);
        double step_length_30_60_ratio = NumberUtils.formatDouble(
                (double)step_length_30_60 / (double)session_count, 2);
        double step_length_60_ratio = NumberUtils.formatDouble(
                (double)step_length_60 / (double)session_count, 2);

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

        sessionAggStatDAO.delete(taskId);
        sessionAggStatDAO.insert(sessionAggStat);
    }

    /**
     * 随机抽取session
     *
     * @param sessionId2AggInfoRDD session抽取
     */
    private static void randomExtractSession(long taskId, JavaPairRDD<String, String> sessionId2AggInfoRDD, JavaPairRDD<String, Row> sessionId2actionRDD) {
        JavaPairRDD<String, String> time2sessionIdRDD = sessionId2AggInfoRDD.mapToPair(tuple -> {
            String aggInfo = tuple._2;
            String startTime = StringUtils.getFieldFromConcatString(aggInfo, "\\|", Constants.FIELD_START_TIME);
            String dataHour = DateUtils.getDateHour(startTime);
            return new Tuple2<>(dataHour, aggInfo);
        });

        // 得到每天每小时的session数量
        Map<String, Long> countMap = time2sessionIdRDD.countByKey();

        // 第二步，使用按时间比例的随机抽取算法，计算出每天每小时要抽取session的索引
        // 将<yyyy-MM-dd_HH, count>格式的map，转换成<yyyy-MM-dd,<HH,count>>的格式
        Map<String, Map<String, Long>> dateHourCountMap =
                new HashMap<>();

        for(Map.Entry<String, Long> countEntry : countMap.entrySet()) {
            String dateHour = countEntry.getKey();
            String date = dateHour.split("_")[0];
            String hour = dateHour.split("_")[1];

            long count = Long.parseLong(String.valueOf(countEntry.getValue()));

            Map<String, Long> hourCountMap = dateHourCountMap.get(date);
            if(hourCountMap == null) {
                hourCountMap = new HashMap<>();
                dateHourCountMap.put(date, hourCountMap);
            }

            hourCountMap.put(hour, count);
        }

        // 开始实现我们的按时间比例随机抽取算法
        // 总共要抽取100个session，先按照天数，进行平分
        int extractNumberPerDay = 100 / dateHourCountMap.size();

        // <date, <hour, (3,5,20,102)>>
        final Map<String, Map<String, List<Integer>>> dateHourExtractMap = new HashMap<>();

        Random random = new Random();

        for (Map.Entry<String, Map<String, Long>> dateHourCountEntry : dateHourCountMap.entrySet()) {
            String date = dateHourCountEntry.getKey();
            Map<String, Long> hourCountMap = dateHourCountEntry.getValue();

            // 计算出这一天的session总数
            long sessionCount = 0L;
            for (long hourCount : hourCountMap.values()) {
                sessionCount += hourCount;
            }

            Map<String, List<Integer>> hourExtractMap = dateHourExtractMap.get(date);
            if (hourExtractMap == null) {
                hourExtractMap = new HashMap<>();
                dateHourExtractMap.put(date, hourExtractMap);
            }

            // 遍历每个小时
            for(Map.Entry<String, Long> hourCountEntry : hourCountMap.entrySet()) {
                String hour = hourCountEntry.getKey();
                Long count = hourCountEntry.getValue();

                // 计算每个小时的session数量，占据当天总session数量的比例，直接乘以每天要抽取的数量
                // 就可以计算出，当前小时需要抽取的session数量
                int hourExtractNumber = (int)(((double)count / (double)sessionCount) * extractNumberPerDay);
                if(hourExtractNumber > count) {
                    hourExtractNumber = count.intValue();
                }

                // 先获取当前小时的存放随机数的list
                List<Integer> extractIndexList = hourExtractMap.get(hour);
                if(extractIndexList == null) {
                    extractIndexList = new ArrayList<>();
                    hourExtractMap.put(hour, extractIndexList);
                }

                // 生成上面计算出来的数量的随机数
                for(int i = 0; i < hourExtractNumber; i++) {
                    int extractIndex = random.nextInt(count.intValue());
                    while(extractIndexList.contains(extractIndex)) {
                        extractIndex = random.nextInt(count.intValue());
                    }
                    extractIndexList.add(extractIndex);
                }
            }
        }

        /*
         * 第三步：遍历每天每小时的session，然后根据随机索引进行抽取
         */

        // 执行groupByKey算子，得到<dateHour, (session, aggInfo)>
        JavaPairRDD<String, Iterable<String>> time2SessionsRDD = time2sessionIdRDD.groupByKey();

        /*
         * 我们用flatMap算子，遍历所有的<dateHour,(session aggInfo)>格式的数据
         * 然后呢，会遍历每天每小时的session
         * 如果发现某个session恰巧在我们指定的这天这小时的随机抽取索引上
         * 那么抽取该session，直接写入MySQL的random_extract_session表
         * 将抽取出来的session id返回回来，形成一个新的JavaRDD<String>
         * 然后最后一步，是用抽取出来的sessionId，去join它们的访问行为明细数据，写入session表
         */
        JavaPairRDD<String, String> extractSessionIdsRDD = time2SessionsRDD.flatMapToPair(tuple -> {
            String dateHour = tuple._1;
            String date = dateHour.split("_")[0];
            String hour = dateHour.split("_")[1];
            Iterator<String> iterator = tuple._2.iterator();

            List<Integer> extractIndexList = dateHourExtractMap.get(date).get(hour);

            ISessionRandomExtractDAO sessionRandomExtractDAO = DAOFactory.getSessionRandomDAO();
            sessionRandomExtractDAO.delete(taskId);

            List<Tuple2<String, String>> extractSessionIds = new ArrayList<>();
            int index = 0;
            while (iterator.hasNext()) {
                String sessionAggInfo = iterator.next();

                if (extractIndexList.contains(index)) {
                    String sessionId = StringUtils.getFieldFromConcatString(
                            sessionAggInfo, "\\|", Constants.FIELD_SESSION_ID);

                    // 将数据写入MySQL
                    SessionRandomExtract sessionRandomExtract = new SessionRandomExtract();
                    sessionRandomExtract.setTaskId(taskId);
                    sessionRandomExtract.setSessionId(sessionId);
                    sessionRandomExtract.setStartTime(StringUtils.getFieldFromConcatString(
                            sessionAggInfo, "\\|", Constants.FIELD_START_TIME));
                    sessionRandomExtract.setSearchKeywords(StringUtils.getFieldFromConcatString(
                            sessionAggInfo, "\\|", Constants.FIELD_SEARCH_KEYWORDS));
                    sessionRandomExtract.setClickCategoryIds(StringUtils.getFieldFromConcatString(
                            sessionAggInfo, "\\|", Constants.FIELD_CLICK_CATEGORY_IDS));

                    sessionRandomExtractDAO.insert(sessionRandomExtract);

                    // 将sessionId加入list
                    extractSessionIds.add(new Tuple2<>(sessionId, sessionId));
                }

                index++;
            }

            return extractSessionIds.iterator();
        });

        /*
         * 第四步：获取抽取出来的session的明细数据
         */
        JavaPairRDD<String, Tuple2<String, Row>> extractSessionDetailRDD = extractSessionIdsRDD.join(sessionId2actionRDD);

        sessionDetailDAO.delete(taskId);
        extractSessionDetailRDD.foreach(tuple -> {
            Row row = tuple._2._2;

            SessionDetail sessionDetail = new SessionDetail();
            try {
                sessionDetail.setTaskId(taskId);
                sessionDetail.setUserId(row.getLong(1));
                sessionDetail.setSessionId(row.getString(2));
                sessionDetail.setPageId(row.getLong(3));
                sessionDetail.setActionTime(row.getString(4));
                sessionDetail.setSearchKeyword(row.getString(5));
                sessionDetail.setClickCategoryId(row.getLong(6));
                sessionDetail.setClickProductId(row.getLong(7));
                sessionDetail.setOrderCategoryIds(row.getString(8));
                sessionDetail.setOrderProductIds(row.getString(9));
                sessionDetail.setPayCategoryIds(row.getString(10));
                sessionDetail.setPayProductIds(row.getString(11));
            } catch (Exception e) {
                return;
            }

            sessionDetailDAO.insert(sessionDetail);
        });
    }

    /**
     * 获取top10热门品类
     *
     * @param taskId 任务id
     * @param filteredSessionId2AggInfoRDD RDD
     * @param sessionId2actionRDD RDD
     */
    private static void getTop10Category(long taskId, JavaPairRDD<String, String> filteredSessionId2AggInfoRDD, JavaPairRDD<String, Row> sessionId2actionRDD) {
        /*
         * 第一步：获取符合条件的session访问过的所有品类
         */

        // 获取符合条件的session的访问明细
        JavaPairRDD<String, Row> sessionId2DetailRDD = filteredSessionId2AggInfoRDD.join(sessionId2actionRDD).mapToPair(tuple -> new Tuple2<>(tuple._1, tuple._2._2));

        // 获取session访问过的所有品类id
        // 访问过：指的是，点击过、下单过、支付过的品类
        JavaPairRDD<Long, Long> categoryIdRDD = sessionId2DetailRDD.flatMapToPair(tuple -> {
            Row row = tuple._2;

            List<Tuple2<Long, Long>> list = new ArrayList<>();

            try {
                Long clickCategoryId = row.getLong(6);
                if (clickCategoryId != null) {
                    list.add(new Tuple2<>(clickCategoryId, clickCategoryId));
                }

                String orderCategoryIds = row.getString(8);
                if (orderCategoryIds != null) {
                    String[] orderCategoryIdsSplit = orderCategoryIds.split(",");
                    for (String orderCategoryId : orderCategoryIdsSplit) {
                        list.add(new Tuple2<>(Long.parseLong(orderCategoryId), Long.parseLong(orderCategoryId)));
                    }
                }

                String payCategoryIds = row.getString(10);
                if (payCategoryIds != null) {
                    String[] payCategoryIdsSplit = payCategoryIds.split(",");
                    for (String payCategoryId : payCategoryIdsSplit) {
                        list.add(new Tuple2<>(Long.parseLong(payCategoryId), Long.parseLong(payCategoryId)));
                    }
                }

                return list.iterator();
            } catch (Exception e) {
                return Collections.emptyIterator();
            }
        });

        /*
         * 第二步：计算各品类的点击、下单和支付的次数
         * 访问明细中，其中三种访问行为是：点击、下单和支付
         * 分别来计算各品类点击、下单和支付的次数，可以先对明细数据进行过滤
         * 分别过滤出点击、下单和支付行为，然后通过map、reduceByKey等算子来进行计算
         */

        // 计算各个品类的点击次数
        JavaPairRDD<Long, Long> clickCategoryId2CountRDD = getClickCategoryId2CountRDD(sessionId2DetailRDD);

        // 计算各个品类的下单次数
        JavaPairRDD<Long, Long> orderCategoryId2CountRDD = getOrderCategoryId2CountRDD(sessionId2DetailRDD);

        // 计算各个品类的支付次数
        JavaPairRDD<Long, Long> payCategoryId2CountRDD = getPayCategoryId2CountRDD(sessionId2DetailRDD);

        /*
         * 第三步：join各品类与它的点击、下单和支付的次数
         *
         * categoryIdRDD中，是包含粒所有的符合条件的session，访问过的品类id
         *
         * 上面分别计算出来的三份，各品类的点击、下单和支付的次数，可能不是包含所有品类的
         * 比如，有的品类，就只是被点击过，但是没有人下单和支付
         *
         * 所以，这里，就不能使用join操作，要使用leftOuterJoin操作，就是说，如果categoryIdRDD不能
         * join到自己的某个数据，比如点击、或下单、或支付次数，那么该categoryIdRDD还是要保留下来的
         * 只不过，没有join到的那个数据，就是0
         */
        JavaPairRDD<Long, String> categoryId2CountRDD = joinCategoryAndData(categoryIdRDD, clickCategoryId2CountRDD, orderCategoryId2CountRDD, payCategoryId2CountRDD);

        /*
         * 第四步：自定义二次排序key
         */

        /*
         * 第五步：将数据映射成<CategorySortKey, Info>格式，然后进行二次排序（降序）
         */
        JavaPairRDD<CategorySortKey, String> sortKey2CountRDD = categoryId2CountRDD.mapToPair(tuple -> {
            String countInfo = tuple._2;
            long clickCount = Long.parseLong(Objects.requireNonNull(StringUtils.getFieldFromConcatString(countInfo, "\\|", Constants.FIELD_CLICK_COUNT)));
            long orderCount = Long.parseLong(Objects.requireNonNull(StringUtils.getFieldFromConcatString(countInfo, "\\|", Constants.FIELD_ORDER_COUNT)));
            long payCount = Long.parseLong(Objects.requireNonNull(StringUtils.getFieldFromConcatString(countInfo, "\\|", Constants.FIELD_PAY_COUNT)));

            CategorySortKey sortKey = new CategorySortKey(clickCount, orderCount, payCount);
            return new Tuple2<>(sortKey, countInfo);
        });

        JavaPairRDD<CategorySortKey, String> sortedCategoryCountRDD = sortKey2CountRDD.sortByKey(false);

        /*
         * 第六步：用take(10)取出top10热门品类，并写入MySQL
         */
        ITop10CategoryDAO top10CategoryDAO = DAOFactory.getTop10CategoryDAO();
        top10CategoryDAO.delete(taskId);

        List<Tuple2<CategorySortKey, String>> top10CategoryList =
                sortedCategoryCountRDD.take(10);
        for(Tuple2<CategorySortKey, String> tuple: top10CategoryList) {
            String countInfo = tuple._2;
            long categoryId = Long.parseLong(StringUtils.getFieldFromConcatString(
                    countInfo, "\\|", Constants.FIELD_CATEGORY_ID));
            long clickCount = Long.parseLong(StringUtils.getFieldFromConcatString(
                    countInfo, "\\|", Constants.FIELD_CLICK_COUNT));
            long orderCount = Long.parseLong(StringUtils.getFieldFromConcatString(
                    countInfo, "\\|", Constants.FIELD_ORDER_COUNT));
            long payCount = Long.parseLong(StringUtils.getFieldFromConcatString(
                    countInfo, "\\|", Constants.FIELD_PAY_COUNT));

            Top10Category category = new Top10Category();
            category.setTaskId(taskId);
            category.setCategoryId(categoryId);
            category.setClickCount(clickCount);
            category.setOrderCount(orderCount);
            category.setPayCount(payCount);

            top10CategoryDAO.insert(category);
        }

    }

    /**
     * 获取各品类点击次数RDD
     *
     * @param sessionId2DetailRDD sessionRDD
     * @return 点击次数
     */
    private static JavaPairRDD<Long, Long> getClickCategoryId2CountRDD(JavaPairRDD<String, Row> sessionId2DetailRDD) {
        JavaPairRDD<Long, Long> clickCategoryIdRDD = sessionId2DetailRDD.mapToPair(tuple -> {
            try {
                long clickCategoryId = tuple._2.getLong(6);
                return new Tuple2<>(clickCategoryId, 1L);
            } catch (Exception e) {
                return null;
            }
        }).filter(Objects::nonNull);

        return clickCategoryIdRDD.reduceByKey(Long::sum);
    }

    /**
     * 计算各个品类的下单次数
     *
     * @param sessionId2DetailRDD sessionRDD
     * @return 下单次数
     */
    private static JavaPairRDD<Long, Long> getOrderCategoryId2CountRDD(JavaPairRDD<String, Row> sessionId2DetailRDD) {
        JavaPairRDD<Long, Long> orderCategoryIdRDD = sessionId2DetailRDD.filter(tuple -> tuple._2.getString(8) != null)
                .flatMapToPair(tuple -> {
                    Row row = tuple._2;
                    String orderCategoryIds = row.getString(8);
                    String[] orderCategoryIdsSplit = orderCategoryIds.split(",");

                    List<Tuple2<Long, Long>> list = new ArrayList<>();

                    for (String orderCategoryId : orderCategoryIdsSplit) {
                        list.add(new Tuple2<>(Long.parseLong(orderCategoryId), 1L));
                    }
                    return list.iterator();
                });

        return orderCategoryIdRDD.reduceByKey(Long::sum);
    }

    /**
     * 计算各个品类的支付次数
     *
     * @param sessionId2DetailRDD sessionRDD
     * @return 支付次数
     */
    private static JavaPairRDD<Long, Long> getPayCategoryId2CountRDD(JavaPairRDD<String, Row> sessionId2DetailRDD) {
        JavaPairRDD<Long, Long> payCategoryIdRDD = sessionId2DetailRDD.filter(tuple -> tuple._2.getString(10) != null)
                .flatMapToPair(tuple -> {
                    Row row = tuple._2;
                    String payCategoryIds = row.getString(10);
                    String[] payCategoryIdsSplit = payCategoryIds.split(",");

                    List<Tuple2<Long, Long>> list = new ArrayList<>();

                    for (String payCategoryId : payCategoryIdsSplit) {
                        list.add(new Tuple2<>(Long.parseLong(payCategoryId), 1L));
                    }

                    return list.iterator();
                });

        return payCategoryIdRDD.reduceByKey(Long::sum);
    }

    /**
     * 连接品类RDD
     *
     * @param categoryIdRDD
     * @param clickCategoryId2CountRDD
     * @param orderCategoryId2CountRDD
     * @param payCategoryId2CountRDD
     * @return
     */
    private static JavaPairRDD<Long, String> joinCategoryAndData(JavaPairRDD<Long, Long> categoryIdRDD,
                                                                 JavaPairRDD<Long, Long> clickCategoryId2CountRDD,
                                                                 JavaPairRDD<Long, Long> orderCategoryId2CountRDD,
                                                                 JavaPairRDD<Long, Long> payCategoryId2CountRDD) {
        JavaPairRDD<Long, Tuple2<Long, Optional<Long>>> tmpJoinRDD = categoryIdRDD.leftOuterJoin(clickCategoryId2CountRDD);

        JavaPairRDD<Long, String> tmpMapRDD = tmpJoinRDD.mapToPair(tuple -> {
            long categoryId = tuple._1;
            Optional<Long> optional = tuple._2._2;
            long clickCount = 0L;

            if (optional.isPresent()) {
                clickCount = optional.get();
            }

            String value = Constants.FIELD_CATEGORY_ID + "=" + categoryId + "|" +
                    Constants.FIELD_CLICK_COUNT + "=" + clickCount;

            return new Tuple2<>(categoryId, value);
        });

        tmpMapRDD = tmpMapRDD.leftOuterJoin(orderCategoryId2CountRDD).mapToPair(tuple -> {
            long categoryId = tuple._1;
            String value = tuple._2._1;

            Optional<Long> optional = tuple._2._2;
            long orderCount = 0L;

            if (optional.isPresent()) {
                orderCount = optional.get();
            }

            value = value + "|" + Constants.FIELD_ORDER_COUNT + "=" + orderCount;
            return new Tuple2<>(categoryId, value);
        });

        tmpMapRDD = tmpMapRDD.leftOuterJoin(payCategoryId2CountRDD).mapToPair(tuple -> {
            long categoryId = tuple._1;
            String value = tuple._2._1;

            Optional<Long> optional = tuple._2._2;
            long payCount = 0L;

            if (optional.isPresent()) {
                payCount = optional.get();
            }

            value = value + "|" + Constants.FIELD_PAY_COUNT + "=" + payCount;
            return new Tuple2<>(categoryId, value);
        });

        return tmpMapRDD;
    }

}
