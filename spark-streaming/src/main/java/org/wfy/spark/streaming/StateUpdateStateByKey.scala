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
object StateUpdateStateByKey {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Stated Transform")
    // 初始化上下文
    val ssc = new StreamingContext(conf, batchDuration = Seconds(3))
    // 设置检查点
    ssc.checkpoint("checkpoint")
    // 监听本地9999端口
    val socket: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)

    val mappedDS: DStream[(String, Int)] = socket.map((_, 1))
    //保留之前的状态，与最近的状态结合
    val state: DStream[(String, Int)] = mappedDS.updateStateByKey(
      (seq: Seq[Int], buff: Option[Int]) => {
        val opt: Int = buff.getOrElse(0) + seq.sum
        // 更新完后，再放入到缓冲区
        Option(opt)
      }
    )
    // 打印输出
    state.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
