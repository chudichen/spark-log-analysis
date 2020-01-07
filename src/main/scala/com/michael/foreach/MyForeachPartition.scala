package com.michael.foreach

import com.michael.util.DBUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

object MyForeachPartition {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("MyForeachPartitionApp")
    val sc = new SparkContext(sparkConf)

    val students = new ListBuffer[String]()

    for (i <- 1 to 100) {
      students += "stu: " + i
    }

    val rdd = sc.parallelize(students)

    myForeachPartition(rdd)
    sc.stop()
  }

  def myForeachPartition(rdd: RDD[String]): Unit = {
    rdd.foreachPartition(x => {
      val conn = DBUtils.getConnection()
      println( x + "------------")
      DBUtils.returnConnection(conn)
    })
  }

  def myForeach(rdd: RDD[String]): Unit = {
    rdd.foreach(x => {
      val conn = DBUtils.getConnection()
      println( x + "------------")

      DBUtils.returnConnection(conn)
    })
  }
}
