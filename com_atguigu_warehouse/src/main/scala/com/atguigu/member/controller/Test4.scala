package com.atguigu.member.controller

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SaveMode, SparkSession}

import scala.util.Random

case class Student(id: Long, name: String, age: Int, partition: Int)

case class School(id: Long, name: String, partition: Int)

object Test4 {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "root")
    val sparkConf = new SparkConf().setAppName("test").setMaster("local[*]")
      .set("spark.sql.optimizer.dynamicPartitionPruning.enabled", "true") //关闭dpp
    //      .set("spark.sql.autoBroadcastJoinThreshold","-1")
    val sparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
    val ssc = sparkSession.sparkContext
    ssc.hadoopConfiguration.set("fs.defaultFS", "hdfs://mycluster")
    ssc.hadoopConfiguration.set("dfs.nameservices", "mycluster")
    //    sparkSession.range(1000000).mapPartitions(partitions => {
    //      val random = new Random()
    //      partitions.map(item => Student(item, "name" + item, random.nextInt(100), random.nextInt(100)))
    //    }).write.partitionBy("partition")
    //      .mode(SaveMode.Append)
    //      .saveAsTable("test_student")
    //
    //    sparkSession.range(1000000).mapPartitions(partitions => {
    //      val random = new Random()
    //      partitions.map(item => School(item, "school" + item, random.nextInt(100)))
    //    }).write.partitionBy("partition")
    //      .mode(SaveMode.Append)
    //      .saveAsTable("test_school")
    val result = sparkSession.sql("select a.id,a.name,a.age,b.name from default.test_student a inner join default.test_school b  " +
      " on a.partition=b.partition and b.id<1000 ")
    import sparkSession.implicits._
    result.foreach(item => println(item.get(1)))
    result.foreachPartition((partition:Iterator[Row]) => {
      partition.foreach(println(_))
    })
    while (true) {

    }
  }
}
