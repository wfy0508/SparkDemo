package org.wfy.spark.core.rdd.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @title: RDD_Transform
 * @projectName SparkDemo
 * @description: TODO
 * @author summer
 * @date 2022-02-24 22:31
 */
object RDD_Transform {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD_Transform")
    val sc = new SparkContext(conf)

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))
    val mapRDD: RDD[Int] = rdd.map((num: Int) => num * 2)
    // 简化版本
    val mapRDD1: RDD[Int] = rdd.map(_ * 2)
    mapRDD.collect()
    mapRDD.foreach(println)
    sc.stop()
  }
}
