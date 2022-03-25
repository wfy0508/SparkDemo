package org.wfy.spark.core.caseAnalysis

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @package: org.wfy.spark.core.`case`
 * @author Summer
 * @description 统计热门品类TOP10--代码存在大量shuffle操作，优化此部分
 * @create 2022-03-25 16:00
 * */
object HotCategoryTop10_v3 {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("HotCategoryTop10"))

    val userData: RDD[String] = sc.textFile("data/user_visit_action.txt")

    //TODO: 按照每个品类的点击、下单、支付的量来统计热门品类
    // 需求优化为：先按照点击数排名，靠前的就排名高；如果点击数相同，再比较下单数；下单数再相同，就比较支付数。

    //最终结果(品类ID, (点击数量, 下单数量, 支付数量))
    // 直接将数据结构转换为最终结构的形式
    val transRdd: RDD[(String, (Int, Int, Int))] = userData.flatMap(
      line => {
        val strings: Array[String] = line.split("_")
        if (strings(6) != "-1") {
          // 点击
          List((strings(6), (1, 0, 0)))
        } else if (strings(8) != "null") {
          // 下单
          val cid: Array[String] = strings(8).split(",")
          cid.map(id => (id, (0, 1, 0)))
        } else if (strings(10) != "null") {
          // 支付
          val cid: Array[String] = strings(10).split(",")
          cid.map(id => (id, (0, 0, 1)))
        } else {
          Nil
        }
      }
    )

    val resultRdd: RDD[(String, (Int, Int, Int))] = transRdd.reduceByKey(
      (t1, t2) => {
        (t1._1 + t2._1, t1._2 + t2._2, t1._3 + t2._3)
      }
    )


    // 6 排序取每个品类的前10
    resultRdd.sortBy(_._2, false).take(10).foreach(println)
    // 停止环境
    sc.stop()
  }

}
