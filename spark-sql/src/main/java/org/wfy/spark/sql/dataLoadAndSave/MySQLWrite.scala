package org.wfy.spark.sql.dataLoadAndSave

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession}

import java.util.Properties

/**
 * @package: org.wfy.spark.sql.dataLoadAndSave
 * @author Summer
 * @description ${description}
 * @create 2022-04-01 16:13
 * */
object MySQLWrite {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("MySQL-Write")
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._

    val df: DataFrame = spark.sparkContext.makeRDD(List(("Tom", 50), ("Cat", 60))).toDF("name", "age")
    val ds: Dataset[User] = df.as[User]

    // 通用方式，通过format指定写出类型
    ds.write.format("jdbc")
      .option("url", "jdbc:mysql://node1:3306/spark_test")
      .option("user", "root")
      .option("password", "1004")
      .option("dbtable", "users")
      .mode(SaveMode.Append)
      .save()

    // 通过JDBC方法
    val props = new Properties()
    props.setProperty("user", "root")
    props.setProperty("password", "1004")
    ds.write.mode(SaveMode.Append).jdbc("jdbc:mysql://node1:3306/spark_test", "users", props)

    spark.stop()
  }
}

case class User(name: String, age: Long)
