import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

/**
 * @package:
 * @author Summer
 * @description ${description}
 * @create 2022-04-14 17:23
 * */
object CommonClass {
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
