package org.wfy.spark.core.rdd

import org.apache.spark
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @program: org.wfy.spark.core.rdd
 * @author Summer
 * @description ${description}
 * @create 2022-03-2022/3/21 9:54
 * */
object MNM_Count {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("mnm_count")
    val sc = new SparkContext(conf)

    val mnmFile: RDD[String] = sc.textFile("data/mnm_dataset.csv")
    val header: String = mnmFile.first()
    // 过滤掉第一行
    val mnmFile1: RDD[String] = mnmFile.filter(row => row != header)

    val mapRdd: RDD[((String, String), Int)] = mnmFile1.map(
      line => {
        val mnmData: Array[String] = line.split(",")
        ((mnmData(0), mnmData(1)), mnmData(2).toInt)
      }
    )

    val reduceRdd: RDD[((String, String), Int)] = mapRdd.reduceByKey(_ + _)

    val newMapRdd: RDD[(String, (String, Int))] = reduceRdd.map {
      case ((city, cate), count) =>
        (city, (cate, count))
    }

    val groupRdd: RDD[(String, Iterable[(String, Int)])] = newMapRdd.groupByKey()
    val result: RDD[(String, List[(String, Int)])] = groupRdd.mapValues(
      iter => {
        iter.toList.sortBy(_._2)(Ordering.Int.reverse).take(3)
      }
    )

    result.collect().foreach(println)


    sc.stop()
  }
}
