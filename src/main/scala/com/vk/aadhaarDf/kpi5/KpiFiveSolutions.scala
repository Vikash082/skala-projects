package com.vk.aadhaarDf.kpi5

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.functions.udf


object KpiFiveSolutions {

  def ageRange(age: Int): String =  {

    var range = ""
    if (age <= 10) range = "1 - 10"
    else if (age <= 20) range = "11 - 20"
    else if (age <= 30) range = "21 - 30"
    else if (age <= 40) range = "31 - 40"
    else if (age <= 50) range = "41 - 50"
    else if (age <= 60) range = "51 - 60"
    else if (age <= 70) range = "61 - 70"
    else if (age <= 80) range = "71 - 80"
    else if (age <= 90) range = "81 - 90"
    else if(age <= 100) range = "91 - 100"

    range
  }

  def main(args: Array[String]) = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val session = SparkSession.builder().appName("KpiFiveSolutions").master("local[1]").getOrCreate()

    val data = session.read
      .option("header", "false")
      .option("inferSchema", value = true )
      .csv("in/aadhaar_data.csv")

    import session.implicits._

    val myudf = udf(ageRange(_: Int))

    val topThreeStatesAadhaarGenMale = data
      .groupBy("_c3")
      .agg(
        (functions.sum(functions.when($"_c7" === "M", $"_c9")) * 100 /
          functions.sum("_c9"))
          .alias("AdhaarGeneratedPercentage")
      )
    .orderBy($"AdhaarGeneratedPercentage".desc)
      .limit(3)

    topThreeStatesAadhaarGenMale.show()

    val topThreeFemaleRejDist = data
      .filter($"_c3" isin("Manipur", "Arunachal Pradesh", "Nagaland"))
      .groupBy("_c4")
      .agg(
        (functions.sum(functions.when($"_c7" === "F", $"_c10")) * 100 /
          (functions.sum(functions.when($"_c7" === "F", $"_c9")) + functions.sum(
          functions.when($"_c7" === "F", $"_c10"))
            )).alias("FemaleAadhaarRej"))
      .orderBy($"FemaleAadhaarRej".desc)
      .limit(3)

    topThreeFemaleRejDist.show()

    val topThreeStatesAadhaarGenFemale = data
      .groupBy("_c3")
      .agg(
        (functions.sum(functions.when($"_c7" === "F", $"_c9")) * 100 /
          functions.sum("_c9"))
          .alias("AdhaarGeneratedPercentage")
      )
      .orderBy($"AdhaarGeneratedPercentage".desc)
      .limit(3)

    topThreeStatesAadhaarGenFemale.show()

    val topThreeMaleRejDist = data
      .filter($"_c3" isin("Andaman and Nicobar Islands", "Chhattisgarh", "Goa"))
      .groupBy("_c4")
      .agg(
        (functions.sum(functions.when($"_c7" === "M", $"_c10")) * 100 /
          (functions.sum(functions.when($"_c7" === "M", $"_c9")) +
            functions.sum(functions.when($"_c7" === "M", $"_c10"))
            )
          ).alias("MaleAadhaarRej")
      )
      .orderBy($"MaleAadhaarRej".desc)
      .limit(3)

    topThreeMaleRejDist.show()

    val newDf = data
      .withColumn("AgeGroup", myudf($"_c8"))
/*

    val acceptancePercSummary = newDf
      .groupBy("AgeGroup")
      .agg(
        (functions.sum(functions.when($"_c7" === "M", $"_c9")) * 100 /
          (functions.sum(functions.when($"_c7" === "M", $"_c9")) +
            functions.sum(functions.when($"_c7" === "M", $"_c10"))
            )).alias("AcceptancePercentage")
      )
      .orderBy($"AgeGroup")
    acceptancePercSummary.show()
*/


    val acceptancePercSummary = newDf
      .groupBy("AgeGroup")
      .agg(
        (functions.sum("_c9") * 100 /
          (functions.sum("_c9") +
            functions.sum("_c10")
            )).alias("AcceptancePercentage")
      )
      .orderBy($"AgeGroup")

    acceptancePercSummary.show()

  }

}
