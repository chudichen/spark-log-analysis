package com.michael.group

import com.michael.foreach.MyForeachPartition.myForeachPartition
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

object MyGroupBy {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("MyGroupBy")
    val sc = new SparkContext(sparkConf)

    // wordCount
    val data = sc.textFile("/opt/hadoop-demo/hello-world/hello")
    println(data.partitions.size)
    data.saveAsTextFile("")
      //      .flatMap(_.split(" "))
      //      .map((_, 1))
      ////      .reduceByKey(_ + _)
      //      .groupByKey()
      //      .map(x => (x._1, x._2.sum))
      //      .collect()
      //      .foreach(println)


      sc.stop()
  }
}
