package com.michael.project.mock;

import com.michael.project.util.DateUtils;
import com.michael.project.util.StringUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.util.*;

/**
 * 模拟数据程序
 *
 * @author Michael Chu
 * @since 2020-01-06 17:26
 */
public class MockData {

    /**
     * 模拟数据
     *
     * @param sc {@link JavaSparkContext}
     * @param sqlContext {@link SQLContext}
     */
    public static void mock(JavaSparkContext sc, SQLContext sqlContext) {
        List<Row> rows = new ArrayList<>();
        String[] searchKeywords = new String[]{"火锅", "蛋糕", "重庆辣子鸡", "重庆小面",
                "呷哺呷哺", "新辣道鱼火锅", "国贸大厦", "太古商场", "日本料理", "温泉"};
        String date = DateUtils.getTodayDate();
        String[] actions = new String[]{"search", "click", "order", "pay"};
        Random random = new Random();

        for (int i = 0; i < 100; i++) {
            long userId = random.nextInt(100);

            for (int j = 0; j < 10; j++) {
                String sessionId = UUID.randomUUID().toString().replace("-", "");
                String baseActionTime = date + " " + random.nextInt(23);

                for (int k = 0; k < random.nextInt(100); k++) {
                    long pageId = random.nextInt(10);
                    String actionTime = baseActionTime + ":" + StringUtils.fulFill(String.valueOf(random.nextInt(59))) + ":" + StringUtils.fulFill(String.valueOf(random.nextInt(59)));
                    String searchKeyword = null;
                    Long clickCategoryId = null;
                    Long clickProductId = null;
                    String orderCategoryIds = null;
                    String orderProductIds = null;
                    String payCategoryIds = null;
                    String payProductIds = null;

                    String action = actions[random.nextInt(4)];
                    switch (action) {
                        case "search":
                            searchKeyword = searchKeywords[random.nextInt(10)];
                            break;
                        case "click":
                            clickCategoryId = Long.valueOf(String.valueOf(random.nextInt(100)));
                            clickProductId = Long.valueOf(String.valueOf(random.nextInt(100)));
                            break;
                        case "order":
                            orderCategoryIds = String.valueOf(random.nextInt(100));
                            orderProductIds = String.valueOf(random.nextInt(100));
                            break;
                        case "pay":
                            payCategoryIds = String.valueOf(random.nextInt(100));
                            payProductIds = String.valueOf(random.nextInt(100));
                            break;
                        default:
                    }

                    Row row = RowFactory.create(date, userId, sessionId,
                            pageId, actionTime, searchKeyword,
                            clickCategoryId, clickProductId,
                            orderCategoryIds, orderProductIds,
                            payCategoryIds, payProductIds);
                    rows.add(row);
                }
            }
        }

        JavaRDD<Row> rowsRDD = sc.parallelize(rows);

        StructType schema = DataTypes.createStructType(Arrays.asList(
                DataTypes.createStructField("date", DataTypes.StringType, true),
                DataTypes.createStructField("user_id", DataTypes.LongType, true),
                DataTypes.createStructField("session_id", DataTypes.StringType, true),
                DataTypes.createStructField("page_id", DataTypes.LongType, true),
                DataTypes.createStructField("action_time", DataTypes.StringType, true),
                DataTypes.createStructField("search_keyword", DataTypes.StringType, true),
                DataTypes.createStructField("click_category_id", DataTypes.LongType, true),
                DataTypes.createStructField("click_product_id", DataTypes.LongType, true),
                DataTypes.createStructField("order_category_ids", DataTypes.StringType, true),
                DataTypes.createStructField("order_product_ids", DataTypes.StringType, true),
                DataTypes.createStructField("pay_category_ids", DataTypes.StringType, true),
                DataTypes.createStructField("pay_product_ids", DataTypes.StringType, true)
        ));

        Dataset<Row> df = sqlContext.createDataFrame(rowsRDD, schema).toDF();
        try {
            df.createTempView("user_visit_action");
        } catch (AnalysisException e) {
            e.printStackTrace();
        }

        df.show(1);

        /*
         * ====================================================================================
         */

        rows.clear();
        String[] sexes = new String[]{"male", "female"};
        for (int i = 0; i < 100; i++) {
            // noinspection UnnecessaryLocalVariable
            long userId = i;
            String username = "user" + i;
            String name = "name" + i;
            int age = random.nextInt(60);
            String professional = "professional" + random.nextInt(100);
            String city = "city" + random.nextInt(100);
            String sex = sexes[random.nextInt(2)];

            Row row = RowFactory.create(userId, username, name, age, professional, city, sex);
            rows.add(row);
        }

        rowsRDD = sc.parallelize(rows);

        StructType schema2 = DataTypes.createStructType(Arrays.asList(
                DataTypes.createStructField("user_id", DataTypes.LongType, true),
                DataTypes.createStructField("username", DataTypes.StringType, true),
                DataTypes.createStructField("name", DataTypes.StringType, true),
                DataTypes.createStructField("age", DataTypes.IntegerType, true),
                DataTypes.createStructField("professional", DataTypes.StringType, true),
                DataTypes.createStructField("city", DataTypes.StringType, true),
                DataTypes.createStructField("sex", DataTypes.StringType, true)
        ));

        Dataset df2 = sqlContext.createDataFrame(rowsRDD, schema2);

        df2.show(1);

        df2.registerTempTable("user_info");
    }
}
