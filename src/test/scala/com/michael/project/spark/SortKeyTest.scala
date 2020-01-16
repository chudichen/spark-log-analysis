package com.michael.project.spark

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Michael Chu
 * @since 2020-01-16 16:43
 */
object SortKeyTest {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SortKeyTest").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val arr = Array(
      Tuple2(new SortKey(30, 35, 40), "1"),
      Tuple2(new SortKey(30, 36, 40), "2"),
      Tuple2(new SortKey(30, 36, 41), "3")
    )

    val rdd = sc.parallelize(arr, 1)

    val sortedRdd = rdd.sortByKey(false)

    for (tuple <- sortedRdd.collect()) {
      println(tuple._2)
    }
  }

}
