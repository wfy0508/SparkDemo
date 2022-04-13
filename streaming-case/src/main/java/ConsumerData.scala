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
object ConsumerData {
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

    // transform可周期性执行
    val ds: DStream[((String, String, String), Int)] = AdData.transform(
      rdd => {
        // TODO 周期性获取黑名单数据
        //结果集列表
        val blackList: ListBuffer[String] = ListBuffer[String]()

        val connection: Connection = JDBCUtil.getConnection
        val sql1: PreparedStatement = connection.prepareStatement("select userid from black_list")
        val resultSet: ResultSet = sql1.executeQuery()
        while (resultSet.next()) {
          blackList.append(resultSet.getString(1))
        }

        resultSet.close()
        sql1.close()
        connection.close()

        // 第1步检测是否在黑名单中
        // TODO 判断点击用户是否在黑名单中
        val filterRDD: RDD[AdClickData] = rdd.filter(
          data => {
            !blackList.contains(data)
          }
        )

        // TODO 如果不在黑名单中，就进行数据统计(每个采集周期)
        filterRDD.map(
          data => {
            val sdf = new SimpleDateFormat("yyyy-MM-dd")
            val day: String = sdf.format(new Date(data.ts.toLong))
            val user: String = data.user
            val ad: String = data.ad
            ((day, user, ad), 1)
          }
        ).reduceByKey(_ + _)
      }
    )
    // 第2步检测统计数据，更新数据库
    ds.foreachRDD(
      rdd => {
        rdd.foreach {
          case ((day, user, ad), count) => {
            if (count > 30) {
              // TODO 如果统计后数据超过阈值(30)，就将该用户加入黑名单
              val connection: Connection = JDBCUtil.getConnection
              val sql2: PreparedStatement = connection.prepareStatement(
                """
                  |insert into black_list (userid) values(?)
                  |on duplicate key
                  |update userid = ?
                  |""".stripMargin)
              sql2.setString(1, user)
              sql2.setString(2, user)
              sql2.executeUpdate()
              sql2.close()
              connection.close()
            } else {
              // TODO 如果统计后数据没有超过阈值，就更新当天点击数据
              val connection: Connection = JDBCUtil.getConnection
              val sql3: PreparedStatement = connection.prepareStatement(
                """
                  |select *
                  |from user_ad_count
                  |where dt = ?
                  |  and userid = ?
                  |  and adid = ?
                  |""".stripMargin)
              sql3.setString(1, day)
              sql3.setString(2, user)
              sql3.setString(3, ad)

              // 查询统计表数据
              val resultSet: ResultSet = sql3.executeQuery()
              if (resultSet.next()) {
                // 如果存在数据，那么更新
                val sql4: PreparedStatement = connection.prepareStatement(
                  """
                    |update user_ad_count
                    |set count = count+?
                    |where dt = ?
                    |  and userid = ?
                    |  and adid = ?
                    |""".stripMargin)
                sql4.setInt(1, count)
                sql4.setString(2, day)
                sql4.setString(3, user)
                sql4.setString(4, ad)
                // 执行更新语句
                sql4.executeUpdate()
                sql4.close()

                // 查询执行更新操作之后的数据
                val sql5: PreparedStatement = connection.prepareStatement(
                  """
                    |select  * from user_ad_count
                    |where dt = ?
                    |  and userid = ?
                    |  and adid = ?
                    |  and count > 30
                    |""".stripMargin)
                val resultSet1: ResultSet = sql5.executeQuery()
                if (resultSet1.next()) {
                  // TODO 更新后的点击数据是否超过阈值，超过就将该用户加入黑名单
                  val sql6: PreparedStatement = connection.prepareStatement(
                    """
                      |insert into black_list (userid) values(?)
                      |on duplicate key
                      |update userid = ?
                      |""".stripMargin)
                  sql6.setString(1, user)
                  sql6.setString(2, user)
                  sql6.executeUpdate()
                  sql6.close()

                } else {
                  // 如果不存在数据，就插入数据
                  val sql7: PreparedStatement = connection.prepareStatement(
                    """
                      |insert into user_ad_count(dt, userid, adid, count) values(?,?,?,?)
                      |""".stripMargin)
                  sql7.setString(1, day)
                  sql7.setString(2, user)
                  sql7.setString(3, ad)
                  sql7.setInt(4, count)
                  sql7.executeUpdate()
                  sql7.close()
                }
                sql5.close()
              }
              connection.close()
            }
          }
        }
      }

    )

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