package com.michael.sql

import org.apache.spark.sql.SparkSession

object DataRddApp {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[2]").appName("DataRddApp").getOrCreate()
    val file = "/opt/spark-demo/spark-sql/person"
    val rdd = spark.sparkContext.textFile(file)

    // 需要导入implicit
    import spark.implicits._
    val dataFrame = rdd.map(_.split(",")).map(line => Info(line(0).toInt, line(1), line(2).toInt)).toDF()

    dataFrame.createTempView("person")
    spark.sql("select * from person where age >20").show()

    spark.stop()
  }

  case class Info(id:Int, name:String, age:Int)
}
