package com.atguigu.member.controller

import java.util.concurrent.{ConcurrentLinkedDeque, Executors}

import com.atguigu.util.HiveUtil
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

object Test {

  case class Student(id: Int, name: String, age: Int, schoolId: Int, dt: String)

  case class School(id: Int, name: String, dt: String)

  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "root")
    val sparkConf = new SparkConf().setAppName("dwd_member_import").setMaster("local[*]")
    val sparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
    val ssc = sparkSession.sparkContext
    ssc.hadoopConfiguration.set("fs.defaultFS", "hdfs://mycluster")
    ssc.hadoopConfiguration.set("dfs.nameservices", "mycluster")
    HiveUtil.openDynamicPartition(sparkSession) //开启动态分区
    val ar1 = new ArrayBuffer[Student]()
    val ar2 = new ArrayBuffer[School]()
    val random = new Random();
    //    for (i <- 0 until 5) {
    //      if (i == 0) {
    //        for (i <- 0 until 10000000) { //插入五千万条数据
    //          ar1.append(Student(i, "name" + i, 18, random.nextInt(15000000), "20200805"))
    //        }
    //        import sparkSession.implicits._
    //        val studentData = ssc.makeRDD(ar1).toDF()
    //        studentData.coalesce(1).write.mode(SaveMode.Append).insertInto("default.student")
    //      } else if (i == 1) {
    //        ar1.clear()
    //        for (i <- 10000000 until 20000000) { //插入五千万条数据
    //          ar1.append(Student(i, "name" + i, 18, random.nextInt(15000000), "20200805"))
    //        }
    //        import sparkSession.implicits._
    //        val studentData = ssc.makeRDD(ar1.toArray).toDF()
    //        studentData.coalesce(1).write.mode(SaveMode.Append).insertInto("default.student")
    //      } else if (i == 2) {
    //        ar1.clear()
    //        for (i <- 20000000 until 30000000) { //插入五千万条数据
    //          ar1.append(Student(i, "name" + i, 18, random.nextInt(15000000), "20200805"))
    //        }
    //        import sparkSession.implicits._
    //        val studentData = ssc.makeRDD(ar1).toDF()
    //        studentData.coalesce(1).write.mode(SaveMode.Append).insertInto("default.student")
    //      } else if (i == 3) {
    //        ar1.clear()
    //        for (i <- 30000000 until 40000000) { //插入五千万条数据
    //          ar1.append(Student(i, "name" + i, 18, random.nextInt(15000000), "20200805"))
    //        }
    //        import sparkSession.implicits._
    //        val studentData = ssc.makeRDD(ar1).toDF()
    //        studentData.coalesce(1).write.mode(SaveMode.Append).insertInto("default.student")
    //      } else if (i == 4) {
    //        ar1.clear()
    //        for (i <- 40000000 until 50000000) { //插入五千万条数据
    //          ar1.append(Student(i, "name" + i, 18, random.nextInt(15000000), "20200805"))
    //        }
    //        import sparkSession.implicits._
    //        val studentData = ssc.makeRDD(ar1).toDF()
    //        studentData.coalesce(1).write.mode(SaveMode.Append).insertInto("default.student")
    //      }
    //    }


//    for (i <- 0 until 15000000) { //插入一千五百万条数据
//      ar2.append(School(i, "school" + 1, "20200805"))
//    }
//
//    import sparkSession.implicits._
//    val schoolData = ssc.makeRDD(ar2).toDF()
//    schoolData.coalesce(1).write.mode(SaveMode.Overwrite).insertInto("default.school")

  }

}
