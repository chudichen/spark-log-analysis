package com.michael.sql

import org.apache.spark.sql.SparkSession

object SparkSessionApp {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("SparkSessionApp").master("local[2]").getOrCreate()

    val file = "/opt/bigdata/spark-2.4.4-bin-hadoop2.7/examples/src/main/resources/people.json"
    val data = spark.read.json(file)

    data.printSchema()
    data.show()

    spark.stop()
  }
}
