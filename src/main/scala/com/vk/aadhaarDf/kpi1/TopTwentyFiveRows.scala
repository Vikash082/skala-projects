package com.vk.aadhaarDf.kpi1

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window

object TopTwentyFiveRows {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val session = SparkSession.builder().appName("TopTwentyFiveRows").master("local[2]").getOrCreate()

    val data = session.read
      .option("header", "false")
      .option("inferSchema", value = true )
      .csv("in/aadhaar_data.csv")

    import session.implicits._

    val window = Window.partitionBy("_c2").orderBy($"_c9".desc)
    val res = data.withColumn("rn", sql.functions.row_number.over(window)).where($"rn" < 25)
    res.rdd.map(_.toString()).saveAsTextFile("out/kpi1_adhaar_df")

  }

}
