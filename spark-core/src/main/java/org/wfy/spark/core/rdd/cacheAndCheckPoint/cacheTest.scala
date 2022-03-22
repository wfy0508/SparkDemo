package org.wfy.spark.core.rdd.cacheAndCheckPoint

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @package: org.wfy.spark.core.rdd.cacheAndCheckPoint
 * @author Summer
 * @description ${description}
 * @create 2022-03-22 17:50
 * */
object cacheTest {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("cache")
    val sc = new SparkContext(conf)

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))
    val mapRdd: RDD[Int] = rdd.map(_ * 2)
    //mapRdd.cache()
    //persist可以设定缓存级别
    mapRdd.persist(StorageLevel.MEMORY_ONLY)

    mapRdd.collect().foreach(println)
    sc.stop()
  }

}
