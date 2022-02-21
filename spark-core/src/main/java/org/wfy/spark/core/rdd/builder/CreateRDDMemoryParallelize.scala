package org.wfy.spark.core.rdd.builder

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @title: CreateRDD
 * @projectName SparkDemo
 * @description: 从内存创建RDD
 * @author summer
 * @date 2022-02-21 22:21
 */
object CreateRDDMemoryParallelize {
  def main(args: Array[String]): Unit = {
    // 准备环境
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(conf)

    // 从内存中创建，makeRDD第二个参数表示分区的数量，
    // numSlices可以不传，默认从配置对象中获取配置参数：spark.default.parallelism
    // 如果获取不到，就使用当前运行环境的totalCore
    //conf.set("spark.default.parallelism", "5")
    //val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))

    // 将处理的数据保存为分区文件
    rdd.saveAsTextFile("output")
    sc.stop()
  }
}
