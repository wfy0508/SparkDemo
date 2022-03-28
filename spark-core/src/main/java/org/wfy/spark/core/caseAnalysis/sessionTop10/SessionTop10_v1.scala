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
    // 1 定义环境
    val sc = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("HotCategoryTop10"))
    //2 打开文件
    val userData: RDD[String] = sc.textFile("data/user_visit_action.txt")

    //TODO: TOP10热门品类中最活跃的TOP10 sessionId

    // 3 品类TOP10，其他内容不要
    val categoryTop10: List[String] = hotCategoryTop10(sc, userData)

    // 4 先通过filter操作，获取只有TOP10商品品类的数据
    val filterRdd: RDD[String] = userData.filter(
      line => {
        val data: Array[String] = line.split("_")
        if (categoryTop10.contains(data(6))) {
          true
        } else {
          false
        }
      }
    )

    val groupRdd: RDD[(String, Iterable[(String, Int)])] = filterRdd.map(
      line => {
        val data: Array[String] = line.split("_")
        // 1 先转换为((商品品类ID, SessionId), 1)
        ((data(6), data(2)), 1)
      }
    ).reduceByKey(_ + _) // 2 聚合数据
      .map {
        // 转换结构((商品品类ID, SessionId), sum) => (商品品类ID, (SessionId, sum))
        case ((k1, k2), sum) => {
          (k1, (k2, sum))
        }
      }.groupByKey()

    // 按照sum值进行排序，并取TOP10
    val sortedRdd: RDD[(String, List[(String, Int)])] = groupRdd.mapValues(
      iter => {
        iter.toList.sortBy(_._2)(Ordering.Int.reverse).take(10)
      }
    )

    // 打印输出
    sortedRdd.foreach(println)
    // 停止环境
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
