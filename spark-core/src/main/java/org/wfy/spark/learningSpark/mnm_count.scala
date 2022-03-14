package org.wfy.spark.learningSpark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
 * @program: org.wfy.spark.learningSpark
 * @author Summer
 * @description ${description}
 * @create 2022-03-2022/3/2 10:16
 * */
object mnm_count {
  def main(args: Array[String]): Unit = {
    val sc = SparkSession.builder().master("local[*]").appName("MnMCount").getOrCreate()
    val mnmFile = "file:\\C:\\learning-spark-v2\\mnm_dataset.csv"
    val mnmDF = (
      sc.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(mnmFile)
      )
    val countMnMDF = mnmDF
      .select("State", "Color", "Count")
      .groupBy("State", "Color")
      .agg(count("Count").alias("Total"))
      .orderBy(desc("Total"))

    countMnMDF.show(60)
    println(s"Total Rows = ${countMnMDF.count()}")
    println()

    //通过过滤获得加州的统计数据
    val caCountMnMDF = mnmDF
      .select("State", "Color", "Count")
      .where(col("State") === "CA")
      .groupBy("State", "Color")
      .agg(count("Count").alias("Total"))
      .orderBy(desc("Total"))

    caCountMnMDF.show(60)
    println(s"Total Rows = ${caCountMnMDF.count()}")

  }
}
