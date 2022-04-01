package org.wfy.spark.sql.dataLoadAndSave

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.util.Properties

/**
 * @package: org.wfy.spark.sql.dataLoadAndSave
 * @author Summer
 * @description ${description}
 * @create 2022-04-01 15:32
 * */
object MySQLRead {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("MySQL-Read")
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._

    // 1 通过load方法读取
    spark.read.format("jdbc")
      .option("url", "jdbc:mysql://node1:3306/spark_test")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("user", "root")
      .option("password", "1004")
      .option("dbtable", "users")
      .load()
      .show()

    // 2 通过load的另一种形式读取
    spark.read.format("jdbc")
      .options(Map(
        "url" -> "jdbc:mysql://node1:3306/spark_test?user=root&password=1004",
        "dbtable" -> "users",
        "driver" -> "com.mysql.jdbc.Driver")).load().show()

    // 3 通过属性类读取
    val props = new Properties()
    props.setProperty("user", "root")
    props.setProperty("password", "1004")
    val df: DataFrame = spark.read.jdbc("jdbc:mysql://node1:3306/spark_test", "users", props)
    df.show()

    spark.stop()
  }

}
