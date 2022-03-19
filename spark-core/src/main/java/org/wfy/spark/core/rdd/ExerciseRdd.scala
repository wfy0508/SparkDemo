package org.wfy.spark.core.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @program: org.wfy.spark.core.rdd
 * @author Summer
 * @description ${description}
 * @create 2022-03-2022/3/19 16:15
 * */
object ExerciseRdd {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("AdClickCountTop3")
    val sc = new SparkContext(conf)

    // 1 读取数据集，结构为时间戳 省 市 人 广告
    val text: RDD[String] = sc.textFile("data/Ad_click.txt")

    // 2 提取数据并转换：=>((省, 广告), 1)
    val mapRdd: RDD[((String, String), Int)] = text.map(
      line => {
        val data: Array[String] = line.split(" ")
        ((data(1), data(4)), 1)
      }
    )
    // 3 将转换后的数据分组聚合((省, 广告), 1) => ((省, 广告), sum)
    val reduceRdd: RDD[((String, String), Int)] = mapRdd.reduceByKey(_ + _)
    // 4 将数据结构转换((省, 广告), sum) => (省, (广告, sum))
    val newMapRdd: RDD[(String, (String, Int))] = reduceRdd.map {
      case ((province, ad), sum) => {
        (province, (ad, sum))
      }
    }
    // 5 将转换后的数据进行分区(省, (广告, sum)) => (省, 【(广告1, sum)， (广告2, sum)， (广告3, sum)】)
    val groupRdd: RDD[(String, Iterable[(String, Int)])] = newMapRdd.groupByKey()
    // 6 排序取Top3
    val result: RDD[(String, List[(String, Int)])] = groupRdd.mapValues(
      iter => {
        iter.toList.sortBy(_._2)(Ordering.Int.reverse).take(3)
      }
    )

    result.collect().foreach(println)
    sc.stop()
  }
}
