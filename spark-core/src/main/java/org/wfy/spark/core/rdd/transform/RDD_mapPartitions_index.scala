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
object RDD_mapPartitions_index {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("mapPartitions"))
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)

    // mapPartitionsWithIndex可以获取分区的索引信息
    val mpi: RDD[Int] = rdd.mapPartitionsWithIndex(
      (index, iter) => {
        if (index == 1) {
          iter
        } else {
          Nil.iterator
        }
      }
    )

    mpi.collect().foreach(println)
    sc.stop()
  }
}
