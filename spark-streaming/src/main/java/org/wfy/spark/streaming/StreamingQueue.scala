package org.wfy.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream, ReceiverInputDStream}

import scala.collection.mutable

/**
 * @package: org.wfy.spark.streaming
 * @author Summer
 * @description 使用队列来创建DStream，每一个推送到此队列中的RDD，都会作为一个DStream来出来
 * @create 2022-04-03 10:13
 * */
object StreamingQueue {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Streaming Queue")
    // 初始化上下文
    val ssc = new StreamingContext(conf, batchDuration = Seconds(3))
    // 创建RDD队列
    val queue = new mutable.Queue[RDD[Int]]()
    // 创建QueueInputStream
    val inputStream: InputDStream[Int] = ssc.queueStream(queue, oneAtATime = true)
    // 处理队列中的数据
    val mappedStream: DStream[(Int, Int)] = inputStream.map((_, 1))
    val reducedDStream: DStream[(Int, Int)] = mappedStream.reduceByKey(_ + _)
    reducedDStream.print()

    ssc.start()
    // 循环想queue中放入数据
    for (i <- 1 to 5) {
      queue += ssc.sparkContext.makeRDD(1 to 300, 10)
      Thread.sleep(2000)
    }
    ssc.awaitTermination()
  }

}
