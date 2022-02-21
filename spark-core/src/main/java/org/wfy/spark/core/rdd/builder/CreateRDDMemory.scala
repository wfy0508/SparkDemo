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
object CreateRDDMemory {
  def main(args: Array[String]): Unit = {
    // 准备环境
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(conf)

    // 从内存中创建
    val seq: Seq[Int] = Seq[Int](1, 2, 3, 4)
    // parallelize：并行，不好记，使用下面的方法
    // val rdd: RDD[Int] = sc.parallelize(seq)
    // makeRDD底层实现时，调用了parallelize方法
    val rdd: RDD[Int] = sc.makeRDD(seq)
    rdd.collect().foreach(println)
    sc.stop()
  }
}
