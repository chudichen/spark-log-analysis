package com.michael.sink

import java.sql.DriverManager

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SinkApp {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("SinkApp")

    val ssc = new StreamingContext(sparkConf, Seconds(5))

    val lines = ssc.socketTextStream("node0", 6789)
    lines.flatMap(_.split(",")).map((_,1))

    // 将socket接受到的数据写入Mysql中
    createConnection()

    ssc.start()
    ssc.awaitTermination()
  }

  def createConnection():Unit = {
    Class.forName("com.mysql.cj.jdbc.Driver")
    DriverManager.getConnection("jdbc://hadoop000:3306/interview", "root", "root")
  }

}
