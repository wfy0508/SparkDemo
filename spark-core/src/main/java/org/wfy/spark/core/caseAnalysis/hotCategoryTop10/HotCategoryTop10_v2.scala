package org.wfy.spark.core.caseAnalysis.hotCategoryTop10

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @package: org.wfy.spark.core.`case`
 * @author Summer
 * @description 统计热门品类TOP10---优化cogroup部分
 * @create 2022-03-25 16:00
 * */
object HotCategoryTop10_v2 {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("HotCategoryTop10"))

    val userData: RDD[String] = sc.textFile("data/user_visit_action.txt")
    // 使用较多直接缓存
    userData.cache()

    //TODO: 按照每个品类的点击、下单、支付的量来统计热门品类
    // 需求优化为：先按照点击数排名，靠前的就排名高；如果点击数相同，再比较下单数；下单数再相同，就比较支付数。

    // 1 首先取出点击数据，并统计每个品类的点击数量
    val clickRdd: RDD[String] = userData.filter(
      (line: String) => {
        val strings: Array[String] = line.split("_")
        // 如果点击的品类ID和产品ID为-1，表示数据不是点击数据
        strings(6) != "-1"
      }
    )

    val clickActionRdd: RDD[(String, Int)] = clickRdd.map(
      (line: String) => {
        val strings: Array[String] = line.split("_")
        (strings(6), 1)
      }
    ).reduceByKey(_ + _)

    // 2 再取出下单数据，并统计每个品类的下单数量
    val orderRdd: RDD[String] = userData.filter(
      (line: String) => {
        val strings: Array[String] = line.split("_")
        // 针对于下单行为，一次可以下单多个商品，所以品类ID和产品ID可以是多个，id之间采用逗号分隔，如果本次不是下单行为，则数据采用null表示
        strings(8) != "null"
      }
    )
    // 使用flatMap将下单中的每个品类统计出来
    val orderActionRdd: RDD[(String, Int)] = orderRdd.flatMap(
      (line: String) => {
        val strings: Array[String] = line.split("_")
        val cid: String = strings(8)
        val cids: Array[String] = cid.split(",")
        //orderid => 1,2,3
        // 输出((1, 1), (2, 1), (3, 1))
        cids.map((_, 1))
      }
    ).reduceByKey(_ + _)

    // 3 最后取出支付数据，并统计每个品类的支付数量
    val payRdd: RDD[String] = userData.filter(
      line => {
        val strings: Array[String] = line.split("_")
        strings(10) != "null"
      }
    )

    val payActionRdd: RDD[(String, Int)] = payRdd.flatMap(
      line => {
        val strings: Array[String] = line.split("_")
        val payCates: String = strings(10)
        val cate: Array[String] = payCates.split(",")
        cate.map((_, 1))
      }
    ).reduceByKey(_ + _)

    // 4 使用cogroup方法合并每个品类点击、下单和支付数据
    // 目标结果为 (category_x, (click, order, pay))
    //val groupRdd: RDD[(String, (Iterable[Int], Iterable[Int], Iterable[Int]))] = clickActionRdd.cogroup(orderActionRdd, payActionRdd)
    //!!!
    // 使用cogroup时，如果rdd之间的分区方式不一致，可能会造成shuffle操作，这样计算消耗资源较多
    //!!!
    // 可以先转换rdd结构
    //(品类ID, 点击数量) => (品类ID, (点击数量, 0, 0))
    //(品类ID, 下单数量) => (品类ID, (0, 下单数量, 0))
    //                => (品类ID, (点击数量, 下单数量, 0))
    //(品类ID, 支付数量) => (品类ID, (0, 0, 支付数量))
    //                => (品类ID, (点击数量, 下单数量, 支付数量)) 这样两两组合，得到与最终结果相同的结构

    //最终结果(品类ID, (点击数量, 下单数量, 支付数量))
    val rdd1: RDD[(String, (Int, Int, Int))] = clickActionRdd.map {
      case (cid, cnt) => {
        (cid, (cnt, 0, 0))
      }
    }

    val rdd2: RDD[(String, (Int, Int, Int))] = orderActionRdd.map {
      case (cid, cnt) => {
        (cid, (0, cnt, 0))
      }
    }

    val rdd3: RDD[(String, (Int, Int, Int))] = payActionRdd.map {
      case (cid, cnt) => {
        (cid, (0, 0, cnt))
      }
    }

    // 使用union聚合
    val groupRdd: RDD[(String, (Int, Int, Int))] = rdd1.union(rdd2).union(rdd3)

    // 使用reduceByKey累加数据
    val resultRdd: RDD[(String, (Int, Int, Int))] = groupRdd.reduceByKey {
      (t1, t2) => {
        (t1._1 + t2._1, t1._2 + t2._2, t1._3 + t2._3)
      }
    }

    // 6 排序取每个品类的前10
    resultRdd.sortBy(_._2, false).take(10).foreach(println)
    // 停止环境
    sc.stop()
  }

}
