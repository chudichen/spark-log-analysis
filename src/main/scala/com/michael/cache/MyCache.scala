package com.michael.cache

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

object MyCache {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("MyCache")

    val sc = new SparkContext(sparkConf)

    val students = new ListBuffer[String]()
    for (i <- 0 to 100) {
      students += "Stu: " + i
    }

    val rdd = sc.parallelize(students, 10)

    rdd.cache()
    rdd.persist()

    sc.stop()
  }
}
