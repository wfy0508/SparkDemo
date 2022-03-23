package org.wfy.spark.core.accumulator

import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable

/**
 * @package: org.wfy.spark.core.accumulator
 * @author Summer
 * @description ${description}
 * @create 2022-03-23 14:33
 * */
class WCAccumulator extends AccumulatorV2[String, mutable.Map[String, Long]] {
  var map: mutable.Map[String, Long] = mutable.Map()

  // 累加器是否为初始值
  override def isZero: Boolean = map.isEmpty

  // 复制累加器
  override def copy(): AccumulatorV2[String, mutable.Map[String, Long]] = new WCAccumulator

  // 重置累加器
  override def reset(): Unit = map.clear()

  // 向累加器中增加数据（In）
  override def add(v: String): Unit = {
    // 查询map中是否有相同的单词，有就加1，没有就添加
    map(v) = map.getOrElse(v, 0L) + 1
  }

  // 合并累加器
  override def merge(other: AccumulatorV2[String, mutable.Map[String, Long]]): Unit = {
    val map1 = map
    val map2 = other.value

    map = map1.foldLeft(map2)(
      (innerMap, kv) => {
        innerMap(kv._1) = innerMap.getOrElse(kv._1, 0L) + kv._2
        innerMap
      }
    )
  }

  // 返回累加器结果
  override def value: mutable.Map[String, Long] = {
    map
  }
}
