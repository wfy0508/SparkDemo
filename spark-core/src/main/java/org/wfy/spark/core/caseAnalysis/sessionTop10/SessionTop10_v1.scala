package org.wfy.spark.core.caseAnalysis.sessionTop10

import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}
import org.wfy.spark.core.caseAnalysis.hotCategoryTop10.{categoryStruct, MyAccumulator}

import scala.collection.mutable

/**
 * @package: org.wfy.spark.core.`case`
 * @author Summer
 * @description 统计热门品类TOP10--使用累加器来实现，避免shuffle操作(练习编程)
 * @create 2022-03-25 16:00
 * */
object SessionTop10_v1 {

  def main(args: Array[String]): Unit = {
    // 定义环境
    val sc = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("HotCategoryTop10"))
    // 打开文件
    val userData: RDD[String] = sc.textFile("data/user_visit_action.txt")
    //TODO: 按照每个品类的点击、下单、支付的量来统计热门品类
    // 需求优化为：先按照点击数排名，靠前的就排名高；如果点击数相同，再比较下单数；下单数再相同，就比较支付数。

    // 直接输出品类TOP10，其他内容不要
    val categoryTop10: List[String] = hotCategoryTop10(sc, userData)
    categoryTop10.foreach(println)


    sc.stop()
  }

  def hotCategoryTop10(sc: SparkContext, sourceData: RDD[String]) = {
    // 创建一个累加器对象
    val accumulator = new MyAccumulator
    // 注册累加器
    sc.register(accumulator, "acc")
    // 遍历读入的数据并使用累加器计算每个品类操作的总和
    sourceData.foreach(
      line => {
        val data: Array[String] = line.split("_")
        if (data(6) != "-1") {
          // (data(6), "click")是累加器的输入
          accumulator.add(data(6), "click")
        } else if (data(8) != "null") {
          val cate2: Array[String] = data(8).split(",")
          cate2.foreach(
            id => {
              accumulator.add(id, "order")
            }
          )
        } else if (data(10) != "null") {
          val cate3: Array[String] = data(10).split(",")
          cate3.foreach(
            id => {
              accumulator.add(id, "pay")
            }
          )
        }
      }
    )

    // 提取累加器的值
    val accValue: mutable.Map[String, categoryStruct] = accumulator.value
    //categoryStruct中已包含商品品类，多以剔除外部String
    val categoryStructs: mutable.Iterable[categoryStruct] = accValue.map(_._2)
    // 对categoryStructs进行排序，由于categoryStructs是可迭代的，不可直接排序，所以先转为List
    // 自定义排序方式，使用sortWith
    val result: List[categoryStruct] = categoryStructs.toList.sortWith(
      (left, right) => {
        if (left.clickCnt > right.clickCnt) {
          true
        }
        else if (left.clickCnt == right.clickCnt) {
          if (left.orderCnt > right.orderCnt) {
            true
          }
          else if (left.orderCnt == right.orderCnt) {
            left.payCnt > right.payCnt
          } else {
            false
          }
        }
        else {
          false
        }
      }
    )
    result.take(10).map(
      line => {
        line.categoryID
      }
    )
  }

}
