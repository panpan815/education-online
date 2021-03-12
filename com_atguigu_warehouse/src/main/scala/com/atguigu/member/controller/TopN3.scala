package com.atguigu.member.controller

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window

object TopN3 {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "root")
    val sparkConf = new SparkConf().setAppName("dwd_member_import").setMaster("local[*]")
    val sparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
    val ssc = sparkSession.sparkContext
    ssc.hadoopConfiguration.set("fs.defaultFS", "hdfs://mycluster")
    ssc.hadoopConfiguration.set("dfs.nameservices", "mycluster")

    //查询用户表、商品表、城市表
    val user_vist_df = sparkSession.sql("select *from dianshang.user_visit_action")
    val producer_info_df = sparkSession.sql("select *from dianshang.product_info")
    val city_info_df = sparkSession.sql("select *from dianshang.city_info")

    //1.查询所有点击记录，并和城市表产品做内连接  .join的两种方式
    //2.计算每个区域的点解量
    //3.对每个商品的点击量倒叙排序
    //4.取出top3

    //使用rank等函数需要导报
    import org.apache.spark.sql.functions._
    val t1 = user_vist_df.join(city_info_df, Seq("city_id"))
      .join(producer_info_df, user_vist_df("click_product_id") === producer_info_df("product_id"))
      .where("click_product_id>-1").groupBy("area", "product_name")
      .count().withColumn("rk", rank().over(Window.partitionBy("area").orderBy(desc("count"))))
      .where("rk<=3")
  }
}
