package org.wfy.spark.core.caseAnalysis

import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
 * @package: org.wfy.spark.core.`case`
 * @author Summer
 * @description 统计热门品类TOP10--使用union中还使用reduceByKey，还是存在shuffle操作，可使用累加器来避免shuffle操作
 * @create 2022-03-25 16:00
 * */
object HotCategoryTop10_v4 {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("HotCategoryTop10"))

    val userData: RDD[String] = sc.textFile("data/user_visit_action.txt")

    //TODO: 按照每个品类的点击、下单、支付的量来统计热门品类
    // 需求优化为：先按照点击数排名，靠前的就排名高；如果点击数相同，再比较下单数；下单数再相同，就比较支付数。

    val acc = new HotCateAcc
    sc.register(acc, "hotAcc")
    userData.foreach(
      line => {
        val strings: Array[String] = line.split("_")
        if (strings(6) != "-1") {
          acc.add((strings(6), "clickType"))
        } else if (strings(8) != "null") {
          val strings1: Array[String] = strings(8).split(",")
          strings1.foreach(
            id => {
              acc.add((id, "orderType"))
            }
          )
        }
        else if (strings(10) != "null") {
          val strings1: Array[String] = strings(10).split(",")
          strings1.foreach(
            id => {
              acc.add((id, "payType"))
            }
          )
        }
      }
    )

    val accValue: mutable.Map[String, HotCategory] = acc.value
    val categories: mutable.Iterable[HotCategory] = accValue.map(_._2)
    val sortedList: List[HotCategory] = categories.toList.sortWith(
      (left, right) => {
        if (left.clickCnt > right.clickCnt) {
          true
        }
        if (left.clickCnt == right.clickCnt) {
          if (left.orderCnt > right.orderCnt) {
            true
          }
          if (left.orderCnt == right.orderCnt) {
            left.payCnt > right.payCnt
          } else {
            false
          }
        } else {
          false
        }
      }
    )

    // 6 排序取每个品类的前10
    sortedList.take(10).foreach(println)
    // 停止环境
    sc.stop()
  }

  //定义一个样例类
  case class HotCategory(prodId: String, var clickCnt: Int, var orderCnt: Int, var payCnt: Int)

  // 定义一个累加器
  // IN: (String, String) -->第一个String指商品的品类，第二个String指类型（clickType, orderType, payType）
  // OUT: mutable.Map[String, HotCategory]-->第一个String指商品的品类，HotCategory指类型求和值（商品品类, clickSum, orderSum, paySum）
  class HotCateAcc extends AccumulatorV2[(String, String), mutable.Map[String, HotCategory]] {

    private val hcMap: mutable.Map[String, HotCategory] = mutable.Map[String, HotCategory]()

    override def isZero: Boolean = {
      hcMap.isEmpty
    }

    override def copy(): AccumulatorV2[(String, String), mutable.Map[String, HotCategory]] = {
      new HotCateAcc
    }

    override def reset(): Unit = {
      hcMap.clear()
    }
    //IN: (String, String) -->第一个String指商品的品类，第二个String指类型（clickType, orderType, payType）
    override def add(v: (String, String)): Unit = {
      val prodId: String = v._1
      val actionType: String = v._2
      val category: HotCategory = hcMap.getOrElse(prodId, HotCategory(prodId, 0, 0, 0))
      if (actionType == "clickType") {
        category.clickCnt += 1
      } else if (actionType == "orderType") {
        category.orderCnt += 1
      } else if (actionType == "payType") {
        category.payCnt += 1
      }
      hcMap.update(prodId, category)
    }

    override def merge(other: AccumulatorV2[(String, String), mutable.Map[String, HotCategory]]): Unit = {
      val map1: mutable.Map[String, HotCategory] = this.hcMap
      val map2: mutable.Map[String, HotCategory] = other.value

      map2.foreach {
        case (cid, hc) => {
          val category: HotCategory = map1.getOrElse(cid, HotCategory(cid, 0, 0, 0))
          category.clickCnt += hc.clickCnt
          category.orderCnt += hc.orderCnt
          category.payCnt += hc.payCnt
          map1.update(cid, category)
        }
      }
    }

    override def value: mutable.Map[String, HotCategory] = hcMap
  }

}
