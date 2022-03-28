package org.wfy.spark.framework.controller

import org.apache.spark.rdd.RDD
import org.wfy.spark.framework.common.TController
import org.wfy.spark.framework.service.WordCountService

/**
 * @package: org.wfy.spark.framework.controller
 * @author Summer
 * @description 控制层
 * @create 2022-03-28 14:35
 * */
class WordCountController extends TController{
  // 要实现controller可以访问service，先创建一个service对象
  private val wordCountService = new WordCountService

  def dispatch() = {
    val result: RDD[(String, Int)] = wordCountService.dataAnalysis()
    result.foreach(println)
  }
}
