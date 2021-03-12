package com.atguigu.member.controller

import com.atguigu.util.HiveUtil
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object Test3 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("dws_sellcourse_import")
      .set("spark.sql.autoBroadcastJoinThreshold", "-1") //把广播join先禁止了
      .set("spark.sql.adaptive.enabled", "true") //开启aqe
      .set("spark.sql.adaptive.skewJoin.enabled", "true") //开启aqe倾斜join 默认开启
      .set("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "2") //设置中文数默认值5 官网有误
      .set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "10m") //默认值256m
      .set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "8m")  //推荐每个分区的数据量
      .set("spark.sql.adaptive.coalescePartitions.enabled", "false") //先关闭动态合并分区
    val sparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
    val ssc = sparkSession.sparkContext
    ssc.hadoopConfiguration.set("fs.defaultFS", "hdfs://mycluster")
    ssc.hadoopConfiguration.set("dfs.nameservices", "mycluster")
    HiveUtil.openDynamicPartition(sparkSession)
    HiveUtil.openCompression(sparkSession)
    val course = sparkSession.sql("select courseid,coursename,status,pointlistid,majorid,chapterid,chaptername,edusubjectid," +
      "edusubjectname,teacherid,teachername,coursemanager,money,dt,dn from dwd.dwd_sale_course")
    val shoppingCart = sparkSession.sql("select courseid,orderid,coursename,discount,sellmoney,createtime,dt,dn from dwd.dwd_course_shopping_cart")
      .drop("coursename")
      .withColumnRenamed("discount", "cart_discount")
      .withColumnRenamed("createtime", "cart_createtime")
    val result = course.join(shoppingCart, Seq("courseid", "dt", "dn"), "right")
    result.foreach(item => print(item.getAs[String]("coursename")))
  }
}
