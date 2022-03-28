package org.wfy.spark.framework.common

import org.apache.spark.{SparkConf, SparkContext}
import org.wfy.spark.framework.controller.WordCountController
import org.wfy.spark.framework.util.EnvUtil

/**
 * @package: org.wfy.spark.framework.common
 * @author Summer
 * @description ${description}
 * @create 2022-03-28 15:21
 * */
trait TApplication {
  // op：控制抽象
  def start(master: String = "local[*]", appName: String = "Application")(op: => Unit) = {
    // 环境准备
    val conf: SparkConf = new SparkConf().setMaster(master).setAppName(appName)
    val sc = new SparkContext(conf)
    // 将sc放入ThreadLocal中
    EnvUtil.put(sc)

    try {
      op
    } catch {
      case ex => println(ex.getMessage)
    }

    // 关闭环境
    sc.stop()
    // 用完后清除sc
    EnvUtil.clear()
  }
}
