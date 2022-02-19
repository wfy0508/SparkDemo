package org.wfy.spark.core.wordcount

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @title: WordCount
 * @projectName SparkDemo
 * @description: TODO
 * @author summer
 * @date 2022-02-19 18:08
 */
object WordCount2 {
  def main(args: Array[String]): Unit = {
    // 1 连接Spark环境
    val conf = new SparkConf().setMaster("local").setAppName("WordCount")
    val context = new SparkContext(conf)

    // 2 读取数据
    val lines = context.textFile("data")
    // 3 将数据按照分隔符（空格）对读取的行进行分割
    val words = lines.flatMap(_.split(" "))
    // 4 按照单词进行分组计数
    val wordOne = words.map {
      word => (word, 1)
    }

    // 相同Key的数据，可以对value进行reduce聚合
    //wordOne.reduceByKey((x, y) => x+y))
    //wordOne.reduceByKey((x, y)=>x+y)
    val wordCount = wordOne.reduceByKey(_ + _)
    // 5 将wordCount收集
    val tuples = wordCount.collect()

    // 6 打印输出
    tuples.foreach(println)

    context.stop()

  }
}
