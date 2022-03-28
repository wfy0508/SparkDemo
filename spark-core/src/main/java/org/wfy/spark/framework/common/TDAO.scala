package org.wfy.spark.framework.common

import org.apache.spark.rdd.RDD

/**
 * @package: org.wfy.spark.framework.common
 * @author Summer
 * @description ${description}
 * @create 2022-03-28 15:38
 * */
trait TDAO {
  def readFile(path: String):Any
}
