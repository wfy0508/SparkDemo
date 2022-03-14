package org.wfy.spark.learningSpark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

/**
 * @program: org.wfy.spark.learningSpark
 * @author Summer
 * @description ${description}
 * @create 2022-03-2022/3/2 16:00
 * */
object SfFireCallData {
  def main(args: Array[String]): Unit = {
    val sc = SparkSession.builder().master("local[*]").appName("sf_fire_call_data").getOrCreate()
    //原始报警数据
    val sfFireFile = "file:\\C:\\learning-spark-v2\\sf-fire\\sf-fire-calls.csv"
    // 定义表结构
    val fireSchema = StructType(Array(
      StructField("CallNumber", IntegerType, true),
      StructField("UnitID", StringType, true),
      StructField("IncidentNumber", IntegerType, true),
      StructField("CallType", StringType, true),
      StructField("CallDate", StringType, true),
      StructField("WatchDate", StringType, true),
      StructField("CallFinalDisposition", StringType, true),
      StructField("AvailableDtTm", StringType, true),
      StructField("Address", StringType, true),
      StructField("City", StringType, true),
      StructField("Zipcode", IntegerType, true),
      StructField("Battalion", StringType, true),
      StructField("StationArea", StringType, true),
      StructField("Box", StringType, true),
      StructField("OriginalPriority", StringType, true),
      StructField("Priority", StringType, true),
      StructField("FinalPriority", IntegerType, true),
      StructField("ALSUnit", BooleanType, true),
      StructField("CallTypeGroup", StringType, true),
      StructField("NumAlarms", IntegerType, true),
      StructField("UnitType", StringType, true),
      StructField("UnitSequenceInCallDispatch", IntegerType, true),
      StructField("FirePreventionDistrict", StringType, true),
      StructField("SupervisorDistrict", StringType, true),
      StructField("Neighborhood", StringType, true),
      StructField("Location", StringType, true),
      StructField("RowID", StringType, true),
      StructField("Delay", FloatType, true)
    ))

    val fireDF = sc.read.schema(fireSchema).option("header", "true").csv(sfFireFile)
    fireDF.show(10)
    println(fireDF.count())

    //写出为parquet文件
    val ParquetOutputPath = "file:\\C:\\learning-spark-v2\\parquet"
    fireDF.write.format("parquet").save(ParquetOutputPath)
    sc.stop()
  }
}
