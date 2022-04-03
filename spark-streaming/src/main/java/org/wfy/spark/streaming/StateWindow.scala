package org.wfy.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @package: org.wfy.spark.streaming
 * @author Summer
 * @description ${description}
 * @create 2022-04-03 16:00
 * */
object StateWindow {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("State Window")
    val ssc = new StreamingContext(conf, Seconds(3))
    val socket: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)
    val mappedDs: DStream[(String, Int)] = socket.map((_, 1))
    // 6秒一个窗口
    val windowDs: DStream[(String, Int)] = mappedDs.window(Seconds(6))
    val result: DStream[(String, Int)] = windowDs.reduceByKey(_ + _)

    result.print()
    ssc.start()
    ssc.awaitTermination()
  }

}
