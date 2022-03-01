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
object RDD_mapPartitions {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("mapPartitions"))
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)

    // mapPartitions：可以以分区为单位进行数据转换操作，
    // 但是将整个分区数据加载进内存进行操作，如果处理完的数据不进行释放，存在对象的引用。当数据量较大时，可能引起内存的溢出
    val mp: RDD[Int] = rdd.mapPartitions(
      iter => {
        println("<<<<<<")
        iter.map(_ * 2)
      }
    )

    mp.collect().foreach(println)
    sc.stop()
  }
}
