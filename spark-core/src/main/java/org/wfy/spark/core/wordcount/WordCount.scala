package org.wfy.spark.core.wordcount

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @title: WordCount
 * @projectName SparkDemo
 * @description: TODO
 * @author summer
 * @date 2022-02-19 18:08
 */
object WordCount {
  def main(args: Array[String]): Unit = {
    // 1 连接Spark环境
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val context = new SparkContext(conf)

    // 2 读取数据
    val lines: RDD[String] = context.textFile("data")
    // 3 将数据按照分隔符（空格）对读取的行进行分割
    val words: RDD[String] = lines.flatMap(_.split(" "))
    // 4 按照单词进行分组
    val wordGroup: RDD[(String, Iterable[String])] = words.groupBy(word => word)
    // 5 对分组后的数据进行转换
    val value: RDD[(String, Int)] = wordGroup.map {
      case (word, list) => {
        (word, list.size)
      }
    }
    // 6 打印输出
    value.foreach(println)

    context.stop()

  }
}
