package com.michael.sql

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

/**
 * SqlContext的使用
 */
object SparkSqlContextApp {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("SparkSqlContextApp")
    val sc = new SparkContext(conf)

    // 1. 创建SqlContext
    val sqlContext = new SQLContext(sc)
    val file = "/opt/bigdata/spark-2.4.4-bin-hadoop2.7/examples/src/main/resources/people.json"
    val data = sqlContext.read.format("json").load(file)
    data.printSchema()
    data.show()

    sc.stop()
  }

}
