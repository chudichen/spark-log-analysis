package com.michael.serial


import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

object MySerial {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("MySerial")
      .registerKryoClasses(Array(classOf[Info]))
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .registerKryoClasses(Array(classOf[Info]))
    val sc = new SparkContext(sparkConf)

    val infos = new ArrayBuffer[Info]()
    val names = Array[String]("Tom", "Jack", "Lucy")
    val genders = Array[String]("male", "female")
    val addresses = Array[String]("shenzhen", "beijing", "shanghai", "chengdu")

    for (i <- 1 to 1000000) {
      val name = names(Random.nextInt(3))
      val gender = genders(Random.nextInt(2))
      val address = addresses(Random.nextInt(4))
      infos += Info(name, Random.nextInt(50), gender, address)
    }

    val rdd = sc.parallelize(infos)

    rdd.persist(StorageLevel.MEMORY_ONLY_SER)

    println(rdd.count())
    Thread.sleep(60 * 1000)

    sc.stop()
  }

  case class Info(name:String, age:Int, gender:String, address:String)
}
