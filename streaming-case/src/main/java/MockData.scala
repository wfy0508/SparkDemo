import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}

import java.util.Properties
import scala.collection.mutable.ListBuffer
import scala.util.Random

/**
 * @package:
 * @author Summer
 * @description 模拟生产数据
 * @create 2022-04-07 19:50
 * */
object MockData {
  def main(args: Array[String]): Unit = {
    // 创建一个生产者
    val producer: KafkaProducer[String, String] = kafkaConfig("node1:9092")

    while (true) {
      mockData().foreach(
        data => {
          //向Kafka传送数据
          //val record = new ProducerRecord[String, String]("streaming", data)
          //producer.send(record)
          println(data)
        }
      )
      Thread.sleep(1000)
    }

  }

  // Kafka生产者配置
  def kafkaConfig(broker: String) = {
    val props = new Properties()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, broker)
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    new KafkaProducer[String, String](props)
  }

  // 模拟随机产生数据
  def mockData() = {
    val list = ListBuffer[String]()
    val areaList = ListBuffer[String]("华北", "华东", "华南")
    val cityList = ListBuffer[String]("北京", "上海", "广州")

    for (i <- 1 to 30) {
      val area = areaList(new Random().nextInt(3))
      val city = cityList(new Random().nextInt(3))
      var userid = new Random().nextInt(10) + 1
      var adid = new Random().nextInt(6) + 1
      list.append(s"${System.currentTimeMillis()} ${area} ${city} ${userid} ${adid}")
    }
    list
  }

}
