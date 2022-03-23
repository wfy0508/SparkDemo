package org.wfy.spark.core.rdd.partitioner

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, Partitioner, SparkConf, SparkContext}

/**
 * @package: org.wfy.spark.core.rdd.partitioner
 * @author Summer
 * @description ${description}
 * @create 2022-03-22 19:50
 * */
object partitionTest {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("partitioner"))

    val rdd: RDD[(String, Int)] = sc.makeRDD(List(
      ("apple", 10),
      ("orange", 20),
      ("pear", 30),
      ("apple", 40)
    ), 3)

    val newRdd: RDD[(String, Int)] = rdd.partitionBy(new MyPartitioner)
    rdd.saveAsTextFile("data/output")
    sc.stop()
  }


  class MyPartitioner extends Partitioner {
    override def numPartitions: Int = 3

    override def getPartition(key: Any): Int = {
      key match {
        case "apple" => 0
        case "pear" => 1
        case _ => 2
      }
    }
  }
}
