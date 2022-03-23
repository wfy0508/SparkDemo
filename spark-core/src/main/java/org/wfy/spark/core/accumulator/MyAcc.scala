package org.wfy.spark.core.accumulator

import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @package: org.wfy.spark.core.accumuator
 * @author Summer
 * @description ${description}
 * @create 2022-03-23 14:25
 * */
object MyAcc {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("Accumulator"))
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))

    val sum: LongAccumulator = sc.longAccumulator("sum")
    rdd.foreach(
      num => {
        sum.add(num)
      }
    )
    println("sum = " + sum.value)
  }
}
