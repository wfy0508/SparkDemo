import com.sun.corba.se.pept.broker.Broker
import com.sun.corba.se.spi.ior.ObjectKey
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord, KafkaConsumer}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.KafkaUtils.createDirectStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

import java.sql.{Connection, PreparedStatement, ResultSet}
import java.text.SimpleDateFormat
import java.util.{Date, Properties}
import scala.collection.mutable.ListBuffer

/**
 * @title: GetAndAnalysisData
 * @projectName SparkDemo
 * @description: 从Kafka获取广告点击数据，并分析处理
 * @author summer
 * @date 2022-04-10 21:43
 */
object ConsumerData1 {
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
        val line: String = kafkaData.value()
        val data: Array[String] = line.split(" ")
        AdClickData(data(0), data(1), data(2), data(3), data(4))
      }
    )
    AdData.print()
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
case class AdClickData1(ts: String, area: String, city: String, user: String, ad: String)