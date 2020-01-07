package com.michael.sql

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

object StructRdd {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[2]").appName("StructRdd").getOrCreate()
    val file = "/opt/spark-demo/spark-sql/person"
    val rdd = spark.sparkContext.textFile(file)

    val infoRdd = rdd.map(_.split(",")).map(line => Row(line(0).toInt, line(1), line(2).toInt))

    val structType = StructType(Array(StructField("id", IntegerType),StructField("name", StringType),StructField("age", IntegerType)))
    val infoDF = spark.createDataFrame(infoRdd, structType)

    infoDF.createTempView("person")
    spark.sql("select * from person").show()
  }

}
