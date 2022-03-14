package org.wfy.spark.core.rdd.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @program: org.wfy.spark.core.rdd.transform
 * @author Summer
 * @description ${description}
 * @create 2022-03-2022/3/14 14:59
 * */
object RDD_Map {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("Map")
    val sc = new SparkContext(conf)
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))

    // map：每个值乘以2
    val double_rdd: RDD[Int] = rdd.map(
      num => {
        num * 2
      }
    )
    double_rdd.collect().foreach(println)

    println("--mapPartitions--------------------")
    // mapPartition: 已分区为单位发送到节点进行处理，可以并行执行
    val mapPartition_rdd: RDD[Int] = rdd.mapPartitions(
      data => {
        data.map(_ * 2)
      }
    )
    mapPartition_rdd.collect().foreach(println)

    // 取出每个分区的最大值
    println("取出每个分区的最大值：")
    val rdd1: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)
    val partitionMax: RDD[Int] = rdd1.mapPartitions(
      iter => {
        // 需要传入一个迭代器，那就将最大值转化为一个List，并返回一个迭代器
        List(iter.max).iterator
      }
    )
    partitionMax.collect().foreach(println)
    println("--mapPartitionsWithIndex--------------------")
    val indexRdd: RDD[Int] = rdd1.mapPartitionsWithIndex(
      (index, iter) => {
        if (index == 1) {
          iter
        } else {
          Nil.iterator
        }
      }
    )
    indexRdd.collect().foreach(println)

    println("--flatMap--------------------")
    val rdd2: RDD[Any] = sc.makeRDD(List(List(1, 2, 3), List(4, 5, 6), 7))
    val flatRdd: RDD[Any] = rdd2.flatMap(
      data => {
        data match {
          case list: List[_] => list
          case i => List(i)
        }
      }
    )
    flatRdd.collect().foreach(println)

    println("--groupBy--------------------")
    val rdd3: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 1)
    val groupByRdd: RDD[(Int, Iterable[Int])] = rdd3.groupBy(_ % 2)
    groupByRdd.collect().foreach(println)

    // 按照首字母来分组
    println("--groupBy-按照首字母来分组-------------------")
    val testRdd: RDD[String] = sc.makeRDD(List("Spark", "Scala", "Flink", "Hello", "Hi"))
    val firstWordSplit: RDD[(String, Iterable[String])] = testRdd.groupBy(_.substring(0, 1))
    firstWordSplit.collect().foreach(println)

    //wordcount
    println("--groupBy-wordcount-------------------")
    val rdd4: RDD[String] = sc.makeRDD(List("hello spark", "hello world", "hello scala"))
    val splitRdd: RDD[String] = rdd4.flatMap(_.split(" "))
    val groupRdd: RDD[(String, Iterable[String])] = splitRdd.groupBy(word => word)
    val mapValuesRdd: RDD[(String, Int)] = groupRdd.mapValues(iter => iter.size)
    groupRdd.collect().foreach(println)
    println("\n")
    mapValuesRdd.collect().foreach(println)


    sc.stop()
  }
}
