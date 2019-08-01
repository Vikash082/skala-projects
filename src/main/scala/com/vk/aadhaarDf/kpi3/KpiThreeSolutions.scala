package com.vk.aadhaarDf.kpi3

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SparkSession, functions}

object KpiThreeSolutions {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val session = SparkSession.builder().appName("KpiThreeSolutions").master("local[1]").getOrCreate()

    val data = session.read
      .option("header", "false")
      .option("inferSchema", value = true )
      .csv("in/aadhaar_data.csv")

    import session.implicits._

    val topThreeStates = data.select("_c3", "_c9")
      .groupBy("_c3")
      .agg(functions.sum("_c9")
      .alias("TotalAadhaarGenerated"))
      .orderBy($"TotalAadhaarGenerated".desc)
      .limit(3)

    topThreeStates.show()

    val topThreePrivAgencies = data.select("_c2", "_c9")
      .groupBy("_c2")
      .agg(functions.sum("_c9")
        .alias("TotalAadhaarGenerated"))
      .orderBy($"TotalAadhaarGenerated".desc)
      .limit(3)

    topThreePrivAgencies.show()

    val emailAndMobileProviders = data.filter($"_c12".isNotNull && $"_c11".isNotNull).count()
    println("EmaiAndMobileProviders: - " + emailAndMobileProviders)

    val topThreeDistricts = data.groupBy("_c4")
      .agg(functions.sum($"_c9" + $"_c10")
      .alias("TotalEnrolment"))
      .orderBy($"TotalEnrolment".desc)
      .limit(3)

    topThreeDistricts.show()

    val aadhaarGeneratedInStates = data.groupBy("_c3")
      .agg(functions.sum("_c10")
      .alias("TotalAdhaarGenerated"))
      .orderBy($"TotalAdhaarGenerated".desc)

    aadhaarGeneratedInStates.show(37)
  }

}
