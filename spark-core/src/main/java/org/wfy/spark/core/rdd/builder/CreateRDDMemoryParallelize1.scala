package org.wfy.spark.core.rdd.builder

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @title: CreateRDD
 * @projectName SparkDemo
 * @description: 从内存创建RDD，数据是怎么进入不同的分区
 * @author summer
 * @date 2022-02-21 22:21
 */
object CreateRDDMemoryParallelize1 {
  def main(args: Array[String]): Unit = {
    // 准备环境
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(conf)


    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))

    // 将处理的数据保存为分区文件
    rdd.saveAsTextFile("output")
    sc.stop()
  }
}
