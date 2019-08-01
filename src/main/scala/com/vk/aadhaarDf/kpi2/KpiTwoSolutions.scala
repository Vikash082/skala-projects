package com.vk.aadhaarDf.kpi2

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.countDistinct

object KpiTwoSolutions {

  def main(args: Array[String]): Unit = {


    Logger.getLogger("org").setLevel(Level.ERROR)
    val session = SparkSession.builder().appName("KpiTwoSolutions").master("local[1]").getOrCreate()

    val data = session.read
      .option("header", "false")
      .option("inferSchema", value = true )
      .csv("in/aadhaar_data.csv")

    import session.implicits._

    val res1 = data.select("_c1").groupBy("_c1").count()
    res1.show()

    val numOfStates = data. select("_c3").distinct().count()
    println("Number of states - " + numOfStates)

    val numOfDistricts = data.select("_c3", "_c4")
      .groupBy("_c3")
      .agg(countDistinct("_c4"))
    numOfDistricts.show()

    val numOfSubDistricts = data.select("_c4", "_c5")
      .groupBy("_c4")
      .agg(countDistinct("_c5"))
    numOfSubDistricts.show()

    val numOfPvtAgencies = data.select("_c3", "_c2")
      .orderBy("_c3")
      .groupBy("_c3", "_c2")

    numOfPvtAgencies.count().show()
  }

}
