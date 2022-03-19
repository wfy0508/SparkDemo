package org.wfy.spark.core.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @program: org.wfy.spark.core.rdd
 * @author Summer
 * @description ${description}
 * @create 2022-03-2022/3/19 16:39
 * */
object ExerciseTest {
  def main(args: Array[String]): Unit = {
    // 创建执行环境
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("AdClick_WriteMySelf")
    val sc = new SparkContext(conf)

    // 1 读取数据源
    // 1647675593 河南 郑州 Tom A
    val text: RDD[String] = sc.textFile("data/Ad_click.txt")

    // 2 从text中提取目标列并做转换
    // 1647675593 河南 郑州 Tom A => ((河南, A), 1)
    val mapRdd: RDD[((String, String), Int)] = text.map(
      line => {
        // 先将读取的行按照空格分割
        val data: Array[String] = line.split(" ")
        // 取出省， 广告，并加上统计量1
        ((data(1), data(4)), 1)
      }
    )

    // 3 对数据进行reduceByKey操作，将相同键的数据求和
    // ((省份, 广告), 1) => ((省份, 广告), sum)
    val reduceRdd: RDD[((String, String), Int)] = mapRdd.reduceByKey(_ + _)

    // 4 转换数据结构
    // ((省份, 广告), sum) => (省份, (广告, sum))
    val newMapRdd: RDD[(String, (String, Int))] = reduceRdd.map {
      // 使用模式匹配进行转换
      case ((province, ads), sum) => {
        (province, (ads, sum))
      }
    }

    // 5 对数据进行group操作，将同省份的数据进行汇聚
    val groupRdd: RDD[(String, Iterable[(String, Int)])] = newMapRdd.groupByKey()

    // 6 将数据进行排序并取top3，这一步只需要操作value即可，使用mapValues
    val resultRdd: RDD[(String, List[(String, Int)])] = groupRdd.mapValues(
      iter => {
        // 迭代器不可排序，将其先转换为List再做排序
        iter.toList.sortBy(_._2)(Ordering.Int.reverse)
      }
    )

    // 7 将结果输出
    resultRdd.collect().foreach(println)

    // 8 停止执行环境
    sc.stop()

  }

}
