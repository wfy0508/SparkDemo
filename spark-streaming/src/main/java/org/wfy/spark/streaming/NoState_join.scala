package org.wfy.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @package: org.wfy.spark.streaming
 * @author Summer
 * @description 有状态转换（保留之前状态）
 * @create 2022-04-03 14:54
 * */
object NoState_join {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("NoState_join")
    // 初始化上下文
    val ssc = new StreamingContext(conf, batchDuration = Seconds(3))
    // 监听本地端口
    val data8888: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 8888)
    val data9999: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)

    val mapped8888: DStream[(String, Int)] = data8888.map((_, 8))
    val mapped9999: DStream[(String, Int)] = data9999.map((_, 9))

    val joinDs: DStream[(String, (Int, Int))] = mapped8888.join(mapped9999)
    joinDs.print()


    ssc.start()
    ssc.awaitTermination()
  }
}
