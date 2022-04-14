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
object Req1_BlackList {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("AD click analysis")
    val ssc = new StreamingContext(conf, Seconds(3))
    val broker: String = "node1:9092"
    val topic: String = "sparkStreaming"

    // 读取Kafka数据，创建DStream
    val value: InputDStream[ConsumerRecord[String, String]] = CommonClass.readKafkaData(ssc, topic, broker)
    // 拆解获取到的数据
    val AdData: DStream[AdClickData] = value.map(
      kafkaData => {
        val line: String = kafkaData.value()
        val data: Array[String] = line.split(" ")
        AdClickData(data(0), data(1), data(2), data(3), data(4))
      }
    )

    // 1 黑名单判断
    // transform可周期性执行
    val ds: DStream[((String, String, String), Int)] = AdData.transform(
      rdd => {
        // 1.1 周期性获取黑名单数据
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

        // 1.2 检测是否在黑名单中
        val filterRDD: RDD[AdClickData] = rdd.filter(
          data => {
            !blackList.contains(data)
          }
        )

        // 1.3 如果不在黑名单中，就进行数据统计(每个采集周期)
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
    ds.print()

    // 2 不在黑名单的后续处理
    ds.foreachRDD(
      rdd => {
        rdd.foreach {
          case ((day, user, ad), count) => {
            // 2.1 如果超过阈值（点击次数大于等于30），直接加入黑名单
            if (count >= 30) {
              val connection: Connection = JDBCUtil.getConnection
              val sql1: PreparedStatement = connection.prepareStatement(
                """
                  |insert into black_list(userid) values(?)
                  |on duplicate key
                  |update userid = ?
                  |""".stripMargin)
              sql1.setString(1, user)
              sql1.setString(2, user)
              sql1.executeUpdate()
              sql1.close()
              connection.close()
            } else {
              // 2.2 不超过阈值，首先查询数据是否存在
              val connection1: Connection = JDBCUtil.getConnection
              val sql2: PreparedStatement = connection1.prepareStatement(
                """
                  |select * from user_ad_count
                  |where dt = ?
                  |  and userid = ?
                  |  and adid = ?
                  |""".stripMargin)
              sql2.setString(1, day)
              sql2.setString(2, user)
              sql2.setString(3, ad)
              val resultSet1: ResultSet = sql2.executeQuery()
              // 2.2.1 存在就直接更新数据
              if (resultSet1.next()) {
                val sql3: PreparedStatement = connection1.prepareStatement(
                  """
                    |update user_ad_count
                    |set count = count+ ?
                    |where dt = ?
                    |  and userid = ?
                    |  and adid = ?
                    |""".stripMargin)
                sql3.setInt(1, count)
                sql3.setString(2, day)
                sql3.setString(3, user)
                sql3.setString(4, ad)
                sql3.executeUpdate()
                sql3.close()

                // 2.2.2 查询更新后的数据
                val sql4: PreparedStatement = connection1.prepareStatement(
                  """
                    |select * from user_ad_count
                    |where dt = ?
                    |  and userid = ?
                    |  and adid = ?
                    |  and count >= 30
                    |""".stripMargin)
                sql4.setString(1, day)
                sql4.setString(2, user)
                sql4.setString(3, ad)
                val resultSet2: ResultSet = sql4.executeQuery()
                // 2.2.3 判断更新后的数据是否超过阈值，超过阈值插入黑名单
                if (resultSet2.next()) {
                  val sql5: PreparedStatement = connection1.prepareStatement(
                    """
                      |insert into black_list(userid) values(?)
                      |on duplicate key
                      |update userid = ?
                      |""".stripMargin)
                  sql5.setString(1, user)
                  sql5.setString(2, user)
                  sql5.executeUpdate()
                  sql5.close()
                }
                resultSet2.close()
                sql4.close()
              } else {
                // 2.3 不存在就插入数据
                val sql6: PreparedStatement = connection1.prepareStatement(
                  """
                    |insert into user_ad_count(dt, userid, adid, count) values(?, ?, ?, ?)
                    |""".stripMargin)
                sql6.setString(1, day)
                sql6.setString(2, user)
                sql6.setString(3, ad)
                sql6.setInt(4, count)
                sql6.executeUpdate()
                sql6.close()
              }
              resultSet1.close()
              sql2.close()
              connection1.close()
            }
          }
        }
      }
    )

    ssc.start()
    ssc.awaitTermination()

  }

}

// 定义一个广告内容的样例类
case class AdClickData(ts: String, area: String, city: String, user: String, ad: String)