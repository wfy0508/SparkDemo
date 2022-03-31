package org.wfy.spark.sql.UDF

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.UserDefinedFunction

/**
 * @package: org.wfy.spark.sql.UDF
 * @author Summer
 * @description UDF
 * @create 2022-03-31 14:14
 * */
object sparkUDF {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("UDF")
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    // 读取json文件，创建DataFrame
    val df: DataFrame = spark.read.json("data/person.json")
    df.createTempView("person")

    // 自定义一个函数
    val addName: UserDefinedFunction = spark.udf.register("addName", (x: String) => "Name: " + x)
    // 使用自定义函数
    spark.sql("select addName(username) from person").show()

    spark.stop()

  }

}
