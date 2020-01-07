package com.michael.sql

import java.sql.DriverManager

object SparkSqlThriftServerApp {

  def main(args: Array[String]): Unit = {
    Class.forName("org.apache.hive.jdbc.HiveDriver")
    val conn = DriverManager.getConnection("jdbc:hive2://localhost:10000")
    val pstat = conn.prepareStatement("select * from emp")
    val rs = pstat.executeQuery()
    while (rs.next()) {
      print(rs.getObject(0))
    }
  }
}
