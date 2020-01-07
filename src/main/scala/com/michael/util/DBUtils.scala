package com.michael.util

object DBUtils {

  def getConnection(): String = {
    "currentConnection is : " + Math.random()
  }

  def returnConnection(conn: String): Unit = {

  }
}
