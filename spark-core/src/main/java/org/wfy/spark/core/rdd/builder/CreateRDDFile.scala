package org.wfy.spark.core.rdd.builder

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * @title: CreateRDDFile
 * @projectName SparkDemo
 * @description: 从文件创建RDD
 * @author summer
 * @date 2022-02-21 22:51
 */
object CreateRDDFile {
  def main(args: Array[String]): Unit = {
    // 准备环境
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(conf)

    // 从文件创建RDD，path可以是目录也可以是具体文件
    val rdd: RDD[String] = sc.textFile("data/words")
    // path也可以使用通配符 *
    //val rdd = sc.textFile("/data/t*.txt")
    rdd.collect().foreach(println)
    sc.stop()
  }
}
