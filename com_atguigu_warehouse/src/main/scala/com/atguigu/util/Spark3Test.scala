package com.atguigu.util

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.storage.StorageLevel

object Spark3Test {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("test")
    sparkConf.set("spark.memory.offHeap.enabled", "true")
    sparkConf.set("spark.memory.offHeap.size", "1G")
    val sparkSession = SparkSession.builder().enableHiveSupport().config(sparkConf).getOrCreate()
    val df = sparkSession.sql("select *from default.student")
    df.persist(StorageLevel.OFF_HEAP)
    df.foreach(item=>println(""))
    while (true) {

    }
  }
}
