import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import java.sql.Connection
import java.text.SimpleDateFormat
import java.util.Date

/**
 * @package:
 * @author Summer
 * @description ${description}
 * @create 2022-04-14 17:20
 * */
object Req2_RealTimeCount {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("AD realtime click count")
    val streamingContext = new StreamingContext(conf, Seconds(3))
    val broker = "node1:9092"
    val kafkaTopic = "sparkStreaming"

    // 从Kafka读取数据
    val kafkaData: InputDStream[ConsumerRecord[String, String]] = CommonClass.readKafkaData(streamingContext, kafkaTopic, broker)
    // 分解组合数据
    val AdData: DStream[AdClickData] = kafkaData.map(
      data => {
        val line: String = data.value()
        val strings: Array[String] = line.split(" ")
        AdClickData(strings(0), strings(1), strings(2), strings(3), strings(4))
      }
    )

    val reduceDs: DStream[((String, String, String, String), Int)] = AdData.map(
      (data: AdClickData) => {
        val sdf = new SimpleDateFormat("yyyy-MM-dd")
        val day: String = sdf.format(new Date(data.ts.toLong))
        val area: String = data.area
        val city: String = data.city
        val ad: String = data.ad
        ((day, area, city, ad), 1)
      }
    ).reduceByKey(_ + _)

    reduceDs.foreachRDD(
      rdd => {
        // 每个分区单独处理
        rdd.foreachPartition(
          iter => {
            // a.获取链接
            val connection: Connection = JDBCUtil.getConnection
            // b.写库
            iter.foreach {
              case ((day, area, city, ad), sum) => {
                val sql1 =
                  """
                    |insert into area_city_ad_count(dt, area, city, adid, count)
                    |values(?, ?, ?, ?, ?)
                    |on duplicate key
                    |update count = count +?
                    |""".stripMargin
                JDBCUtil.executeUpdate(connection, sql1, Array(day, area, city, ad, sum, sum))
              }
            }
            connection.close()
          }
        )
      }
    )
    streamingContext.start()
    streamingContext.awaitTermination()


  }
}
