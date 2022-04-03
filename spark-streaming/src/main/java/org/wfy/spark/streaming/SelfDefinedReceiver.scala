package org.wfy.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.util.Random

/**
 * @package: org.wfy.spark.streaming
 * @author Summer
 * @description 自定义数据采集器
 * @create 2022-04-03 10:38
 * */
object SelfDefinedReceiver {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Self Defined Receiver")
    // 初始化上下文
    val ssc = new StreamingContext(conf, batchDuration = Seconds(3))

    val reds: ReceiverInputDStream[String] = ssc.receiverStream(new MyReceiver)
    reds.print()

    ssc.start()
    ssc.awaitTermination()
  }
}

/**
 * 自定义数据采集器主要继承Receiver
 */
class MyReceiver extends Receiver[String](StorageLevel.MEMORY_ONLY) {
  private var flag: Boolean = true

  // 启动采集器，需要创建一个独立于主线程之外的线程
  override def onStart(): Unit = {
    new Thread(new Runnable {
      override def run(): Unit = {
        while (flag) {
          val data: String = "采集的数据为：" + new Random().nextInt(10).toString
          store(data)
          Thread.sleep(500)
        }
      }
    }).start()
  }

  // 终止采集器的条件
  override def onStop(): Unit = {
    flag = false
  }
}
