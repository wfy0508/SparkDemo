package org.wfy.spark.learningSpark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

/**
 * @program: org.wfy.spark.learningSpark
 * @author Summer
 * @description ${description}
 * @create 2022-03-2022/3/2 14:21
 * */
object DefineSchema {
  def main(args: Array[String]): Unit = {
    val sc = SparkSession.builder().master("local[*]").appName("DefineSchema").getOrCreate()
    val jsonFile = "file:\\C:\\learning-spark-v2\\blogs.json"
    val schema = StructType(Array(
      StructField("Id", IntegerType, false),
      StructField("First", StringType, false),
      StructField("Last", StringType, false),
      StructField("Url", StringType, false),
      StructField("Published", StringType, false),
      StructField("Hits", IntegerType, false),
      StructField("Campaigns", ArrayType(StringType), false)
    ))

    val blogsDF = sc.read.schema(schema).json(jsonFile)
    blogsDF.show(false)
    println(blogsDF.printSchema)
    println(blogsDF.schema)
  }
}
