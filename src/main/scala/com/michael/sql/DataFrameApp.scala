package com.michael.sql

import org.apache.spark.sql.SparkSession

object DataFrameApp {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[2]").appName("DataFrameApp").getOrCreate()
    val df = spark.read.format("jdbc").options(Map("" +
      "url" -> "jdbc:mysql://hadoop000:3306/spark?createDatabaseIfNotExist=true&serverTimezone=UTC&user=root&password=root",
      "dbtable" -> "dept",
      "driver" -> "com.mysql.cj.jdbc.Driver"
    )).load()

    df.show()
    spark.stop()
  }
}
