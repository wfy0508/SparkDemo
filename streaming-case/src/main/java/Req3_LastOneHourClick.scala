import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import java.io.{File, FileWriter, PrintWriter}
import java.text.SimpleDateFormat
import java.util.Date
import scala.collection.mutable.ListBuffer

/**
 * @package:
 * @author Summer
 * @description 最近一小时广告点击数量统计
 * @create 2022-04-15 19:58
 * */
object Req3_LastOneHourClick {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Last one hour click count")
    val streamingContext = new StreamingContext(conf, Seconds(5))
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

    val resultDS: DStream[(Long, Int)] = AdData.map(
      data => {
        val ts: Long = data.ts.toLong
        // 每10秒统计一次
        val newTs = ts / 10000 * 10000
        (newTs, 1)
      }
    ).reduceByKeyAndWindow((x: Int, y: Int) => (x + y), Seconds(60), Seconds(10))

    //resultDS.print()
    resultDS.foreachRDD(
      rdd => {
        val list: ListBuffer[String] = ListBuffer[String]()
        val sortedData: Array[(Long, Int)] = rdd.sortByKey(true).collect()
        sortedData.foreach {
          case (time, cnt) => {
            val str: String = new SimpleDateFormat("yyyy-MM-dd mm:ss").format(new Date(time.toLong))
            list.append(s"""{"时间": "${str}", "点击次数": "${cnt}"}""")
          }
        }

        val writer = new PrintWriter(new FileWriter(new File("C:\\Users\\Summer\\IdeaProjects\\SparkDemo\\data\\Req3_output")))
        writer.println("[" + list.mkString(",") + "]")
        writer.flush()
        writer.close()
      }
    )

    streamingContext.start()
    streamingContext.awaitTermination()
  }
}
