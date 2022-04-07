package org.wfy.spark.streaming

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @package: org.wfy.spark.streaming
 * @author Summer
 * @description 通过SparkStreaming从Kafka读取数据，并将读取过来的数据做简单计算，最终打印到控制台。
 * @create 2022-04-07 20:19
 * */
object GetFromKafka {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Get data from Kafka")
    val ssc = new StreamingContext(conf, batchDuration = Seconds(3))

    // 定义Kafka参数
    val kafkaPara: Map[String, Object] = Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "node1:9092,node2:9092,node3:9092",
      ConsumerConfig.GROUP_ID_CONFIG -> "sparkStreaming",
      "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer"
    )

    // 读取Kafka数据，创建DStream
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe(Set("sparkStreaming"), kafkaPara)
    )

    // 将每条消息的的K,V值取出来
    val valueDS: DStream[String] = kafkaDStream.map(
      record => {
        record.value()
      }
    )

    // 计算WordCount
    valueDS.flatMap(_.split(" "))
      .map((_, 1))
      .reduceByKey(_ + _)
      .print()
    // 开始任务
    ssc.start()
    ssc.awaitTermination()


  }
}
