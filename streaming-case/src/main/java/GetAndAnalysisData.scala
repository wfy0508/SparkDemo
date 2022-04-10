import com.sun.corba.se.pept.broker.Broker
import com.sun.corba.se.spi.ior.ObjectKey
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord, KafkaConsumer}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.KafkaUtils.createDirectStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

import java.util.Properties

/**
 * @title: GetAndAnalysisData
 * @projectName SparkDemo
 * @description: 从Kafka获取广告点击数据，并分析处理
 * @author summer
 * @date 2022-04-10 21:43
 */
object GetAndAnalysisData {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("AD click analysis")
    val ssc = new StreamingContext(conf, Seconds(3))
    val broker: String = "node1:9092"
    val topic: String = "sparkStreaming"


  }

  /**
   * 定义消费者配置
   *
   * @param broker 服务器配置ip和port
   * @return
   */
  def myKafkaConfig(broker: String, topic: String): Map[String, Object] = {
    Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> broker,
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
      ConsumerConfig.GROUP_ID_CONFIG -> topic
    )
  }

  /**
   * 读取Kafka数据，创建DStream
   *
   * @param ssc StreamingContext
   * @param topic Kafka主题
   * @param broker 服务器
   * @return
   */
  def readKafkaData(ssc: StreamingContext, topic: String, broker: String): InputDStream[ConsumerRecord[String, String]] = {
    KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Set("sparkStreaming"), myKafkaConfig(broker, topic))
    )
  }

}
