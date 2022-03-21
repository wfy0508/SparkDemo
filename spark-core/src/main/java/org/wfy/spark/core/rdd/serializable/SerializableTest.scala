package org.wfy.spark.core.rdd.serializable

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @package: org.wfy.spark.core.rdd.serializable
 * @author Summer
 * @description ${description}
 * @create 2022-03-21 15:20
 * */
object SerializableTest {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SerializableTest")
    val sc = new SparkContext(conf)

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))

    val user = new User()

    // rdd算子是在Executor中执行的，rdd算子外的部分在Driver中执行，Driver和Executor之间需要网络传输。
    // 所以在调用时，用到的对象需要可序列化
    rdd.foreach(
      num => {
        println("age=" + (user.age + num))
      }
    )

    sc.stop()
  }


  class User extends Serializable {
    val age: Int = 23;
  }

}
