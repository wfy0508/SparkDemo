package org.wfy.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @package:
 * @author Summer
 * @description ${description}
 * @create 2022-04-01 20:43
 * */
object StreamWordCount {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Streaming WordCount")
    // 初始化上下文
    val ssc = new StreamingContext(conf, Seconds(3))
    // 通过监控端口创建DStream，数据为一行一行进来
    val lineStreams: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)
    // 将数据切分
    val wordStream: DStream[String] = lineStreams.flatMap(_.split(" "))
    // 初始化单词数量
    val tupleWord: DStream[(String, Int)] = wordStream.map((_, 1))
    // 汇总计算
    val result: DStream[(String, Int)] = tupleWord.reduceByKey(_ + _)
    //打印输出
    result.print()
    // 启动StreamingContext
    ssc.start()
    ssc.awaitTermination()

  }

}
