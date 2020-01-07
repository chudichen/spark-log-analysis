package com.michael.repartition

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

object CoalesceAndRepartition {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("CoalesceAndRepartition")

    val sc = new SparkContext(sparkConf)

    val students = new ListBuffer[String]()
    for (i <- 0 to 100) {
      students += "Stu: " + i
    }

    val rdd = sc.parallelize(students, 10)

    println("Raw partition size: " + rdd.partitions.length)

    val newRdd = rdd.repartition(20)

    println("Raw partition size: " + newRdd.partitions.length)

    sc.stop()
  }

}
