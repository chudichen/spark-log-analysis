package com.michael.sql

import org.apache.spark.sql.SparkSession

/**
 * DataFrame中的其他操作
 */
object DataFrameCase {

  // noinspection DuplicatedCode
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[2]").appName("DataFrameCase").getOrCreate()
    val path = "/opt/bigdata/spark-2.4.4-bin-hadoop2.7/examples/src/main/resources/people.csv"
    val dataFrame = spark.read.option("header", "true").option("infoSchema", "true").csv(path)
    import spark.implicits._
    val ds = dataFrame.as[People]

//    ds.show()

    spark.stop()
  }

  case class Info(id:Int, name:String, age:Int)

  case class People(name:Object, age:Int, job:String)
}
