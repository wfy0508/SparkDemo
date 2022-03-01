package org.wfy.spark.core.rdd.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @title: RDDParalize
 * @projectName SparkDemo
 * @description: TODO
 * @author summer
 * @date 2022-03-01 21:43
 */
object RDDParalize {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD Parallelize")
    val sc = new SparkContext(conf)

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)

    // 在只有一个分区时，rdd分区内的执行是有序的，前面一个数据执行完成后，后面一个数据才会执行
    val val1: RDD[Int] = rdd.map(
      num => {
        println(">>>>>>>" + num)
        num
      }
    )

    val val2: RDD[Int] = val1.map(
      num => {
        println("#######" + num)
        num
      }
    )

    val2.collect()
    sc.stop()
  }

}
