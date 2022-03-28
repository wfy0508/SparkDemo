package org.wfy.spark.framework.application

import org.wfy.spark.framework.common.TApplication
import org.wfy.spark.framework.controller.WordCountController


/**
 * @package: org.wfy.spark.framework.application
 * @author Summer
 * @description 应用层
 * @create 2022-03-28 14:34
 * */
object WordCountApplication extends App with TApplication {
  // 启动应用程序
  start() {
    // 要实现application可以访问controller，先创建一个controller对象
    val wordCountController = new WordCountController
    wordCountController.dispatch()
  }

}
