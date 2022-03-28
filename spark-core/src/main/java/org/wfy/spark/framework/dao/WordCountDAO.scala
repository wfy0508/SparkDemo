package org.wfy.spark.framework.dao

import org.apache.spark.rdd.RDD
import org.wfy.spark.framework.util.EnvUtil

/**
 * @package: org.wfy.spark.framework.dao
 * @author Summer
 * @description DAO
 * @create 2022-03-28 14:36
 * */
class WordCountDAO {
  // 读取数据
  def readFile(path: String): RDD[String] = {
    // 将sc从ThreadLocal取出来
    EnvUtil.take().textFile(path)
  }
}
