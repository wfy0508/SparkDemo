package org.wfy.spark.core.rdd.action

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * @program: org.wfy.spark.core.rdd.action
 * @author Summer
 * @description ${description}
 * @create 2022-03-2022/3/18 10:03
 * */
object RddAction {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD_Action")
    val sc = new SparkContext(conf)
    println("--Action----------------------------------------------------------------------------")
    println("--reduce-------------------")
    val rdd17: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5))
    val i: Int = rdd17.reduce(_ + _)
    println(i)

    println("--count-------------------")
    println(rdd17.count())
    println("--first-------------------")
    println(rdd17.first())
    println("--take-------------------")
    println(rdd17.take(2))

    println("--aggregate-------------------")
    val rdd18: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 8)
    val i1: Int = rdd18.aggregate(0)(_ + _, _ + _)
    println(i1)

    println("--aggregate-------------------")
    val i2: Int = rdd18.fold(0)(_ + _)
    println(i2)

    println("--countByKey-------------------")
    val rdd19: RDD[(Int, String)] = sc.makeRDD(List((1, "a"), (1, "a"), (1, "a"), (2, "b"), (3, "c"), (3, "c")))
    val result: collection.Map[Int, Long] = rdd19.countByKey()
    print(result)

  }

}
