package org.wfy.spark.core.rdd.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

import scala.math

import java.io

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
    println("--groupBy-wordCount-------------------")
    val rdd4: RDD[String] = sc.makeRDD(List("hello spark", "hello world", "hello scala"))
    val splitRdd: RDD[String] = rdd4.flatMap(_.split(" "))
    val groupRdd: RDD[(String, Iterable[String])] = splitRdd.groupBy(word => word)
    val mapValuesRdd: RDD[(String, Int)] = groupRdd.mapValues(iter => iter.size)
    groupRdd.collect().foreach(println)
    println("\n")
    mapValuesRdd.collect().foreach(println)

    println("--filter-------------------")
    val rdd5: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))
    val filterRdd: RDD[Int] = rdd5.filter(_ % 2 == 0)
    filterRdd.collect().foreach(println)

    println("--filter-------------------")
    val sampleRdd: RDD[Int] = rdd5.sample(false, 0.4, 12345)
    sampleRdd.collect().foreach(println)

    println("--distinct-------------------")
    val rdd6: RDD[Int] = sc.makeRDD(List(1, 2, 3, 2, 4, 5, 3, 6, 2, 3, 4, 5, 2, 2, 1, 1, 2))
    rdd6.distinct().collect().foreach(print)
    println("\n-----")
    rdd6.distinct(2).collect().foreach(print)
    println()

    println("--coalesce-------------------")
    val rdd7: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 6, 7, 8), 6)
    println("缩减分区之前的分区信息：")
    rdd7.mapPartitionsWithIndex(
      (index, iter) => {
        List(index).iterator
      }
    ).collect().foreach(print(_, " "))
    println()
    val rdd7_1: RDD[Int] = rdd7.coalesce(2)
    println("缩减分区之后的分区信息：")
    rdd7_1.mapPartitionsWithIndex(
      (index, iter) => {
        List(index).iterator
      }
    ).collect().foreach(print(_, " "))
    println()

    println("--coalesce-------------------")
    val rdd8: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 6, 7, 2, 3, 5), 2)
    rdd8.repartition(4).mapPartitionsWithIndex(
      (index, iter) => {
        List(index).iterator
      }
    ).collect().foreach(print(_, " "))
    println()

    println("--orderBy-------------------")
    rdd8.sortBy(x => x, true).collect().foreach(print)

    println("--zip-------------------")
    val rdd9: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)
    val rdd10: RDD[String] = sc.makeRDD(List("5", "6", "7", "8"), 2)
    rdd9.zip(rdd10).collect().foreach(println)

    println("--partitionBy-------------------")
    val rdd11: RDD[(Int, String)] = sc.makeRDD(Array((1, "AAA"), (2, "BBB"), (3, "CCC")), 1)
    println("重分区之前：")
    rdd11.mapPartitionsWithIndex(
      (index, iter) => {
        val part_map = scala.collection.mutable.Map[String, List[Any]]()
        val part_name = "part_" + index
        part_map(part_name) = List[Any]()
        while (iter.hasNext) {
          part_map(part_name) :+= iter.next()
        }
        part_map.iterator
      }
    ).collect().foreach(println)
    // (part_0,List((1,AAA), (2,BBB), (3,CCC)))

    println("重分区之后：")
    rdd11.partitionBy(new HashPartitioner(2)).mapPartitionsWithIndex(
      (index, iter) => {
        val part_map = scala.collection.mutable.Map[String, List[Any]]()
        val part_name = "part_" + index
        part_map(part_name) = List[Any]()
        while (iter.hasNext) {
          part_map(part_name) :+= iter.next()
        }
        part_map.iterator
      }
    ).collect().foreach(println)
    //(part_0,List((2,BBB)))
    //(part_1,List((1,AAA), (3,CCC)))

    println("--reduceByKey-------------------")
    val rdd12: RDD[(String, Int)] = sc.makeRDD(List(("a", 5), ("b", 4), ("c", 3), ("b", 9)))
    rdd12.reduceByKey(_ + _).collect().foreach(println)

    rdd12.map(num => (num._1, 1)).reduceByKey(_ + _).collect().foreach(println)

    println("--groupByKey-------------------")
    rdd12.groupByKey().collect().foreach(println)

    println("--aggregateByKey-------------------")
    rdd12.aggregateByKey(0)(_ + _, _ + _).collect().foreach(println)

    val rdd13: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("a", 2), ("c", 3), ("b", 4), ("c", 5), ("c", 6)), 2)
    rdd13.aggregateByKey(0)(
      (x, y) => scala.math.max(x, y),
      (x, y) => x + y
    ).collect().foreach(println)

    println("--foldByKey-------------------")
    rdd13.foldByKey(0)(_ + _).collect().foreach(println)

    println("--combineByKey-------------------")
    val rdd14: RDD[(String, Int)] = sc.makeRDD(List(("a", 88), ("b", 95), ("a", 91), ("b", 93), ("a", 95), ("b", 98)))
    rdd14.combineByKey(
      (_, 1),
      (acc: (Int, Int), v) => (acc._1 + v, acc._2 + 1),
      (acc1: (Int, Int), acc2: (Int, Int)) => (acc1._1 + acc2._1, acc1._2 + acc2._2)
    ).collect().foreach(println)

    println("--sortByKey-------------------")
    rdd14.sortByKey(true).collect().foreach(println)

    println("--join-------------------")
    val rdd15: RDD[(String, Int)] = sc.makeRDD(List(("a", 88), ("b", 95), ("c", 100)))
    val rdd16: RDD[(String, Int)] = sc.makeRDD(List(("a", 91), ("b", 93)))
    rdd15.join(rdd16).collect().foreach(println)

    println("--leftOuterJoin-------------------")
    rdd15.leftOuterJoin(rdd16).collect().foreach(println)

    println("--cogroup-------------------")
    rdd15.cogroup(rdd16).collect().foreach(println)

    sc.stop()
  }
}
