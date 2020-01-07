package com.michael.sql

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext

object HiveContextApp {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("HiveContextApp")
    val sc = new SparkContext(conf)

    // 1. 创建SqlContext
    val sqlContext = new HiveContext(sc)

    sc.stop()
  }
}
