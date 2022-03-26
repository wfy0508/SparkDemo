package org.wfy.spark.core.caseAnalysis.hotCategoryTop10

import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2
import org.wfy.spark.core.caseAnalysis.hotCategoryTop10.{categoryStruct, MyAccumulator}
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

    val acc = new MyAccumulator
    sc.register(acc, "hotAcc")
    userData.foreach(
      line => {
        val strings: Array[String] = line.split("_")
        if (strings(6) != "-1") {
          acc.add((strings(6), "clickType"))
        } else if (strings(8) != "null") {
          val strings1: Array[String] = strings(8).split(",")
          strings1.foreach(
            (id: String) => {
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

    val accValue: mutable.Map[String, categoryStruct] = acc.value
    val categories: mutable.Iterable[categoryStruct] = accValue.map(_._2)
    val sortedList: List[categoryStruct] = categories.toList.sortWith(
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
}
