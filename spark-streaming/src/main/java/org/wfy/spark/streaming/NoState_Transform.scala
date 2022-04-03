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
object NoState_Transform {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("NoState_Transform")
    // 初始化上下文
    val ssc = new StreamingContext(conf, batchDuration = Seconds(3))
    // 监听本地9999端口
    val socket: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)

    // transform与map比较
    // 执行位置比较（算子内Executor端，算子外Driver端）
    // transform比map多了一个在Driver端执行的步骤
    // Code: Driver端
    val ds1: DStream[String] = socket.transform(
      rdd => {
        // Code: Driver端（周期性执行）
        rdd.map(
          str => {
            // Code: Executor端
            str
          }
        )
      }
    )

    // Code: Driver端
    val ds2: DStream[String] = socket.map(
      data => {
        // Code: Executor端
        data
      }
    )

    ssc.start()
    ssc.awaitTermination()
  }
}
