package org.wfy.spark.sql.UDF

import org.apache.hadoop.mapred.lib.aggregate.UserDefinedValueAggregatorDescriptor
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, Encoders, Row, SparkSession, TypedColumn, functions}
import org.apache.spark.sql.expressions.{Aggregator, MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, DoubleType, IntegerType, LongType, StructField, StructType}
import org.apache.spark.util.AccumulatorV2

/**
 * @package: org.wfy.spark.sql
 * @author Summer
 * @description 计算平均工资
 * @create 2022-03-31 14:29
 * */
object computeAvgSalary {
  def main(args: Array[String]): Unit = {
    // 创建执行环境
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("AvgSalary")
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    // RDD=>DataFrame=>DataSet之间的转换需要导入隐式转换规则，否则无法转换
    // spark不是包名，是上下文对象
    import spark.implicits._

    // 创建RDD
    val rdd: RDD[(String, Int)] = spark.sparkContext.makeRDD(List(("zhangsan", 2000), ("lisi", 3000), ("wangwu", 4000)))
    // 通过map和reduce计算工资总和和员工数量
    val result: (Int, Int) = rdd.map {
      case (name, salary) => {
        (salary, 1)
      }
    }.reduce {
      (t1, t2) => {
        (t1._1 + t2._1, t1._2 + t2._2)
      }
    }
    println("RDD计算平均工资：" + result._1 / result._2)

    // 使用累加器计算均值
    val ac = new MyAc
    spark.sparkContext.register(ac, "acc")

    rdd.map(_._2).foreach(
      num => ac.add(num)
    )

    println("累加器计算平均工资：" + ac.value)

    // 使用弱类型UDAF
    var uDAF = new MyUDAF
    spark.udf.register("weakUDAF", uDAF)
    val df: DataFrame = rdd.toDF("name", "salary")
    df.createTempView("user")
    println("弱类型UDAF计算平均工资：")
    spark.sql("select weakUDAF(salary) from user").show(1)

    // 使用强类型UDAF
    // 先转换DataFrame为Dataset
    val ds1: Dataset[User01] = df.as[User01]
    val myUDF = new MyUDF1
    val col: TypedColumn[User01, Double] = myUDF.toColumn
    println("强类型UDAF计算平均工资：")
    ds1.select(col).show()

    // Spark 3.0优化版
    // 创建UADF函数
    val myUDF2 = new MyUDF2
    // 注册到SparkSql
    spark.udf.register("avgSalary", functions.udaf(myUDF2))
    // 使用聚合函数
    println("Spark3.0-强类型UDAF计算平均工资：")
    spark.sql("select avgSalary(salary) from user").show()


    spark.stop()
  }
}

/**
 * 定义一个累加器类
 */
class MyAc extends AccumulatorV2[Int, Int] {
  var sum: Int = 0
  var count: Int = 0

  override def isZero: Boolean = {
    return sum == 0 || count == 0
  }

  override def copy(): AccumulatorV2[Int, Int] = {
    val ac = new MyAc
    ac.sum = this.sum
    ac.count = this.count
    ac
  }

  override def reset(): Unit = {
    sum = 0
    count = 0
  }

  override def add(v: Int): Unit = {
    sum += v
    count += 1
  }

  override def merge(other: AccumulatorV2[Int, Int]): Unit = {
    other match {
      case o: MyAc => {
        sum += o.sum
        count += o.count
      }
      case _ =>
    }
  }

  override def value: Int = sum / count
}

/**
 * 定义一个弱类型UDAF，自Spark 3.0起不推荐使用
 */
class MyUDAF extends UserDefinedAggregateFunction {
  // 定义输入的类型
  override def inputSchema: StructType = StructType(Array(StructField("salary", IntegerType)))

  // 计算均值的中间变量sum和count
  override def bufferSchema: StructType = StructType(Array(
    StructField("sum", LongType),
    StructField("count", LongType)
  ))

  // 返回均值的数据类型
  override def dataType: DataType = DoubleType

  //稳定性：对于相同的输入是否一直返回相同的输出。
  override def deterministic: Boolean = true

  //缓冲区初始化
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    // sum
    buffer(0) = 0L
    //count
    buffer(1) = 0L
  }

  // 更新缓冲区数据
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    if (!input.isNullAt(0)) {
      buffer(0) = buffer.getLong(0) + input.getInt(0)
      buffer(1) = buffer.getLong(1) + 1
    }
  }

  // 合并数据
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getLong(0) + buffer2.getLong(0)
    buffer1(1) = buffer1.getLong(1) + buffer2.getLong(1)
  }

  // 计算最终结果
  override def evaluate(buffer: Row): Double = {
    buffer.getLong(0).toDouble / buffer.getLong(1)
  }
}

// 输入数据类型
case class User01(name: String, salary: Long)

// 中间变量
case class SalaryBuffer(var sum: Long, var count: Long)

/**
 * 定义一个强类型UDF
 */
class MyUDF1 extends Aggregator[User01, SalaryBuffer, Double] {
  // 初始化值
  override def zero: SalaryBuffer = {
    SalaryBuffer(0L, 0L)
  }

  // 累加数据
  override def reduce(b: SalaryBuffer, a: User01): SalaryBuffer = {
    b.sum += a.salary
    b.count += 1
    b
  }

  // 合并数据
  override def merge(b1: SalaryBuffer, b2: SalaryBuffer): SalaryBuffer = {
    b1.sum += b2.sum
    b1.count += b2.count
    b1
  }

  override def finish(reduction: SalaryBuffer): Double = {
    reduction.sum.toDouble / reduction.count
  }

  //DataSet 默认的编解码器，用于序列化，固定写法。自定义类型就是product自带类型根据类型选择
  override def bufferEncoder: Encoder[SalaryBuffer] = {
    Encoders.product
  }

  override def outputEncoder: Encoder[Double] = {
    Encoders.scalaDouble
  }
}

/**
 * Spark3.0版本可以采用强类型的Aggregator方式代替UserDefinedAggregateFunction
 */
case class Buff(var sum: Long, var cnt: Long)

class MyUDF2 extends Aggregator[Long, Buff, Double] {
  override def zero: Buff = Buff(0, 0)

  override def reduce(b: Buff, a: Long): Buff = {
    b.sum += a
    b.cnt += 1
    b
  }

  override def merge(b1: Buff, b2: Buff): Buff = {
    b1.sum += b2.sum
    b1.cnt += b2.cnt
    b1
  }

  override def finish(reduction: Buff): Double = {
    reduction.sum.toDouble / reduction.cnt
  }

  override def bufferEncoder: Encoder[Buff] = Encoders.product

  override def outputEncoder: Encoder[Double] = Encoders.scalaDouble
}