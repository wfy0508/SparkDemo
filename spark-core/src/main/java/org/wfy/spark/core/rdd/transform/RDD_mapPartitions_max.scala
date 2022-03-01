package org.wfy.spark.core.rdd.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @title: RDD_mapPartitions
 * @projectName SparkDemo
 * @description: TODO
 * @author summer
 * @date 2022-03-01 21:55
 */
object RDD_mapPartitions_max {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("mapPartitions"))
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)

    // max取每个分区的最大值，传入迭代器，返回迭代器（只需要转换为迭代器就行）
    val mp: RDD[Int] = rdd.mapPartitions(
      iter => {
        List(iter.max).iterator
      }
    )

    mp.collect().foreach(println)
    sc.stop()
  }
}
