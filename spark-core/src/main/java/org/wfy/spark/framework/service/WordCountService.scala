package org.wfy.spark.framework.service

import org.apache.spark.rdd.RDD
import org.wfy.spark.framework.common.TService
import org.wfy.spark.framework.dao.WordCountDAO

/**
 * @package: org.wfy.spark.framework.service
 * @author Summer
 * @description 服务层
 * @create 2022-03-28 14:35
 * */
class WordCountService extends TService{
  // 要实现service可以访问DAO，先创建一个DAO对象
  private val wordCountDAO = new WordCountDAO

  def dataAnalysis() = {
    val lines: RDD[String] = wordCountDAO.readFile("data/words")
    val words: RDD[String] = lines.flatMap(_.split(" "))
    val wordGroup: RDD[(String, Iterable[String])] = words.groupBy(word => word)
    val value: RDD[(String, Int)] = wordGroup.map {
      case (word, list) => {
        (word, list.size)
      }
    }
    value
  }
}
