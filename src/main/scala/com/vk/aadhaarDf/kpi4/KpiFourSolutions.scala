package com.vk.aadhaarDf.kpi4

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SparkSession, functions}

object KpiFourSolutions {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val session = SparkSession.builder().appName("KpiFourSolutions").master("local[1]").getOrCreate()

    val data = session.read
      .option("header", "false")
      .option("inferSchema", value = true )
      .csv("in/aadhaar_data.csv")

    import session.implicits._

    val uniquePincode = data.select("_c6").distinct().count()
    println("Unique Pincode:- " + uniquePincode)

    val aadhaarRejected = data
      .filter($"_c3" === "Uttar Pradesh" || $"_c3" === "Maharashtra")
      .groupBy("_c3")
      .agg(functions.sum("_c10")
      .alias("TotalAdhaarGenerated"))
      .orderBy($"TotalAdhaarGenerated".desc)

    aadhaarRejected.show()

  }

}
