import com.sun.corba.se.pept.broker.Broker
import com.sun.corba.se.spi.ior.ObjectKey
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord, KafkaConsumer}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
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

    // 读取Kafka数据，创建DStream
    val value: InputDStream[ConsumerRecord[String, String]] = readKafkaData(ssc, topic, broker)
    // 拆解获取到的数据
    val AdData: DStream[AdClickData] = value.map(
      kafkaData => {
        val data: String = kafkaData.value()
        val strings: Array[String] = data.split(" ")
        AdClickData(strings(0), strings(1), strings(2), strings(3), strings(4))
      }
    )

    // TODO 周期性获取黑名单数据
    // TODO 判断点击用户是否在黑名单中
    // TODO 如果不在黑名单中，就进行数据统计

    // TODO 如果统计后数据超过阈值，就将该用户加入黑名单
    // TODO 如果统计后数据没有超过阈值，就更新当天点击数据
    // TODO 更新后的点击数据是否超过阈值，超过就将该用户加入黑名单

    ssc.start()
    ssc.awaitTermination()

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
   * @param ssc    StreamingContext
   * @param topic  Kafka主题
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

// 定义一个广告内容的样例类
case class AdClickData(ts: String, area: String, city: String, user: String, ad: String)