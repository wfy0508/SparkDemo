package org.wfy.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
 * @package: org.wfy.spark.sql
 * @author Summer
 * @description ${description}
 * @create 2022-03-30 15:54
 * */
object sparkSqlBasic {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("spark-sql-basic")
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    // RDD=>DataFrame=>DataSet之间的转换需要导入隐式转换规则，否则无法转换
    // spark不是包名，是上下文对象
    import spark.implicits._

    // 读取json文件，创建DataFrame
    val df: DataFrame = spark.read.json("data/person.json")
    //df.show()

    df.createTempView("user")
    // SQL语法风格
    //spark.sql("select * from user").show()

    //DSL语法风格
    //df.select("username", "age").show()

    //创建RDD
    val rdd1: RDD[(Int, String, Int)] = spark.sparkContext.makeRDD(List((1, "zhangsan", 20), (2, "wangwu", 30), (3, "lisi", 40)))
    println("原始RDD：")
    rdd1.collect().foreach(println)

    // RDD=>DataFrame
    val df1: DataFrame = rdd1.toDF("id", "name", "age")
    println("RDD=>DataFrame：")
    df1.show()

    // RDD=>DataSet
    val ds1: Dataset[(Int, String, Int)] = rdd1.toDS()
    println("RDD=>DataSet：")
    ds1.show()

    // RDD=>DataSet
    rdd1.map {
      case (id, name, age) => User(id, name, age)
    }.toDS()

    // DataFrame=>DataSet，通过样例类转换
    val ds2: Dataset[User] = df1.as[User]
    println("DataFrame=>DataSet：")
    ds2.show()

    // DataSet=>DataFrame
    val df2: DataFrame = ds1.toDF()

    // DataSet=>RDD
    val rdd2: RDD[User] = ds2.rdd

    // DataFrame=>RDD
    val rdd3: RDD[Row] = df2.rdd
    //得到的额RDD类型为Row，提供getXxx来获取字段的值，索引从0开始
    rdd3.foreach(a => println(a.getString(1)))

    spark.stop()
  }

}

case class User(id: Int, name: String, age: Int)