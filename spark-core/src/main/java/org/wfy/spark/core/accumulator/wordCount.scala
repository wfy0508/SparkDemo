package org.wfy.spark.core.accumulator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @package: org.wfy.spark.core.accumulator
 * @author Summer
 * @description ${description}
 * @create 2022-03-23 14:57
 * */
object wordCount {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("wordCount"))

    val rdd: RDD[String] = sc.makeRDD(List("hello", "spark", "scala"))

    val acc = new WCAccumulator()
    sc.register(acc, "wordCount")

    rdd.foreach(
      num => {
        acc.add(num)
      }
    )

    println(acc.value)
    sc.stop()
  }
}
