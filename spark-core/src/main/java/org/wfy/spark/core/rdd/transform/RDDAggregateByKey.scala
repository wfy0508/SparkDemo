package org.wfy.spark.core.rdd.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @program: org.wfy.spark.core.rdd.transform
 * @author Summer
 * @description 求平均值
 * @create 2022-03-2022/3/19 10:05
 * */
object RDDAggregateByKey {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("aggregateByKey")
    val sc = new SparkContext(conf)
    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("a", 2), ("b", 3), ("b", 4), ("b", 5), ("a", 6)))
    val newRdd: RDD[(String, (Int, Int))] = rdd.aggregateByKey((0, 0))(
      (t, v) => {
        (t._1 + v, t._2 + 1)
      },
      (t1, t2) => {
        (t1._1 + t2._1, t1._2 + t2._2)
      }
    )

    val result: RDD[(String, Int)] = newRdd.mapValues {
      case (num, cnt) => {
        num / cnt
      }
    }
    result.collect().foreach(println)
    sc.stop()
  }
}
