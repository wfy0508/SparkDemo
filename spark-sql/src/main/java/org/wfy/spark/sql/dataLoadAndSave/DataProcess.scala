package org.wfy.spark.sql.dataLoadAndSave

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * @package: org.wfy.spark.sql.dataLoadAndSave
 * @author Summer
 * @description 数据加载和保存
 * @create 2022-04-01 14:18
 * */
object DataProcess {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Data Process")
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    // 1读取
    // 1.1直接读取
    val df1: DataFrame = spark.read.json("data/person.json")
    // 1.2指定加载数据的类型
    val df2: DataFrame = spark.read.format("json").json("data/person.json")
    // 1.3可以直接使用 文件格式.`文件路径` 的方式来加载数据
    val df3: DataFrame = spark.sql("select * from json.`data/person.json`")
    //format默认格式为parquet
    df1.write.format("parquet").mode("append").save("data/output")
    spark.stop()
  }

}
