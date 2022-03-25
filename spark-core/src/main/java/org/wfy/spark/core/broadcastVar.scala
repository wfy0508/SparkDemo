package org.wfy.spark.core

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.plans.logical.BROADCAST
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
 * @package: org.wfy.spark.core
 * @author Summer
 * @description 使用广播变量
 * @create 2022-03-24 16:15
 * */
object broadcastVar {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("broadcastVar"))
    val rdd1: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("b", 2), ("c", 3)))
    val rdd2: RDD[(String, Int)] = sc.makeRDD(List(("a", 4), ("b", 5), ("c", 6)))
    // 使用join，可能会产生笛卡尔乘积，导致落盘数据几何数量增加
    val joinRdd: RDD[(String, (Int, Int))] = rdd1.join(rdd2)
    joinRdd.collect().foreach(println)
    println("-----------------------")

    val map1: mutable.Map[String, Int] = mutable.Map(("a", 4), ("b", 5), ("c", 6))
    //使用map去遍历rdd1中的数据，相同Key的数据放在一起
    val mapValue: RDD[(Any, (Int, Int))] = rdd1.map {
      case (k, v) => {
        val i: Int = map1.getOrElse(k, 0)
        (k, (v, i))
      }
    }

    mapValue.collect().foreach(println)

    //使用广播变量
    println("--broadcast---------------------")
    val rdd3: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("b", 2), ("c", 3), ("d", 4)), 4)
    val list = List(("a", 4), ("b", 5), ("c", 6), ("d", 7))
    // broadcast为只读数据
    val broadcast: Broadcast[List[(String, Int)]] = sc.broadcast(list)
    val resultRdd: RDD[(String, (Int, Int))] = rdd3.map {
      case (key, value) => {
        var num = 0
        for ((k, v) <- broadcast.value) {
          if (k == key) {
            num = v
          }
        }
        (key, (value, num))
      }
    }
    resultRdd.collect().foreach(println)


    sc.stop()
  }


}
