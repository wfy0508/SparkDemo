package org.wfy.spark.core.rdd.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @title: RDD_mapPartitions
 * @projectName SparkDemo
 * @description: 针对不同的类型，采用模式匹配处理
 * @author summer
 * @date 2022-03-01 21:55
 */
object RDD_mapPartitions_match {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("mapPartitions"))
    val rdd: RDD[Any] = sc.makeRDD(List(List(1, 2), 3, List(4, 5)))

    val flatRDD = rdd.flatMap(
      data => {
        data match {
          case list:List[_] => list
          case dat =>List (dat)
        }
      }
    )

    flatRDD.collect().foreach(println)
    sc.stop()
  }
}
