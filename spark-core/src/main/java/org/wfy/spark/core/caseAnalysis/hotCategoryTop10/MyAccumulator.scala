package org.wfy.spark.core.caseAnalysis.hotCategoryTop10

import org.apache.spark.util.AccumulatorV2
import org.wfy.spark.core.caseAnalysis.hotCategoryTop10.categoryStruct

import scala.collection.mutable

/**
 * @package: org.wfy.spark.core.caseAnalysis.hotCategoryTop10
 * @author Summer
 * @description ${description}
 * @create 2022-03-26 16:32
 * */
/**
 * 定义一个累加器，用户计算每个品类的点击、下单和购买次数
 * IN: (String, String) --(商品品类, 操作类型（点击/下单/支付))
 * OUT: mutable.Map(String, categoryStruct)--(商品品类， 样例类)
 */
class MyAccumulator extends AccumulatorV2[(String, String), mutable.Map[String, categoryStruct]] {
  // 定义一个Map，用于计算输出
  private val map: mutable.Map[String, categoryStruct] = mutable.Map[String, categoryStruct]()

  override def isZero: Boolean = {
    map.isEmpty
  }

  override def copy(): AccumulatorV2[(String, String), mutable.Map[String, categoryStruct]] = {
    new MyAccumulator
  }

  override def reset(): Unit = {
    map.clear()
  }

  override def add(v: (String, String)): Unit = {
    // 品类ID
    val categoryID: String = v._1
    // 操作类型
    val actionType: String = v._2
    val struct: categoryStruct = map.getOrElse(categoryID, categoryStruct(categoryID, 0, 0, 0))
    if (actionType == "click") {
      struct.clickCnt += 1
    } else if (actionType == "order") {
      struct.orderCnt += 1
    } else if (actionType == "pay") {
      struct.payCnt += 1
    }
    map.update(categoryID, struct)
  }

  override def merge(other: AccumulatorV2[(String, String), mutable.Map[String, categoryStruct]]): Unit = {
    val map1: mutable.Map[String, categoryStruct] = this.map
    val map2: mutable.Map[String, categoryStruct] = other.value
    map2.foreach {
      case (categoryId, cs) => {
        // 从map1中先获取当前key是否存在
        val struct: categoryStruct = map1.getOrElse(categoryId, categoryStruct(categoryId, 0, 0, 0))
        // 对应操作类型次数相加
        struct.clickCnt += cs.clickCnt
        struct.orderCnt += cs.orderCnt
        struct.payCnt += cs.payCnt
        map1.update(categoryId, struct)
      }
    }
  }

  override def value: mutable.Map[String, categoryStruct] = map
}
