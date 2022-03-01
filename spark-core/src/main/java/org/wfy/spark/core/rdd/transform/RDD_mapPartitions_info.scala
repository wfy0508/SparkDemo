package org.wfy.spark.core.rdd.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @title: RDD_mapPartitions
 * @projectName SparkDemo
 * @description: 获取数据所在的分区信息
 * @author summer
 * @date 2022-03-01 21:55
 */
object RDD_mapPartitions_info {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("mapPartitions"))
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))

    // mapPartitionsWithIndex可以获取分区的索引信息
    val mpRDD: RDD[(Int, Int)] = rdd.mapPartitionsWithIndex(
      (index, iter) => {
        iter.map(
          num => {
            (index, num)
          }
        )
      }
    )

    mpRDD.collect().foreach(println)
    sc.stop()
  }
}
