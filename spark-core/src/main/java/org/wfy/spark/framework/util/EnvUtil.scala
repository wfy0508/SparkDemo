package org.wfy.spark.framework.util

import org.apache.spark.SparkContext

/**
 * @package: org.wfy.spark.framework.util
 * @author Summer
 * @description ${description}
 * @create 2022-03-28 15:46
 * */
object EnvUtil {
  private val scLocal = new ThreadLocal[SparkContext]()

  def put(sc: SparkContext) = {
    scLocal.set(sc)
  }

  def take() = {
    scLocal.get()
  }

  def clear() = {
    scLocal.remove()
  }
}
