package org.wfy.spark.core.rdd.builder

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @title: CreateRDDFile
 * @projectName SparkDemo
 * @description: 从文件创建RDD
 * @author summer
 * @date 2022-02-21 22:51
 */
object CreateRDDFile1 {
  def main(args: Array[String]): Unit = {
    // 准备环境
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(conf)

    // 从文件创建RDD，使用wholeTextFiles，会打印出路径
    // RDD[(String, String)] 逗号前为文件路径，逗号后为文件内容
    val rdd: RDD[(String, String)] = sc.wholeTextFiles("data")
    // path也可以使用通配符 *
    //val rdd = sc.textFile("/data/t*.txt")
    rdd.collect().foreach(println)
    sc.stop()
  }
}
