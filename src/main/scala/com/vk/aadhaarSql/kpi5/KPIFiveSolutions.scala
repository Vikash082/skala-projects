package com.vk.aadhaarSql.kpi5

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object KPIFiveSolutions {

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

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val session = SparkSession.builder().appName("KPIFiveSolutions").master("local[1]").getOrCreate()

    val data = session.read
      .option("header", "false")
      .option("inferSchema", value = true )
      .csv("in/aadhaar_data.csv")

    data.createOrReplaceTempView("aadhaar")

    session.udf.register("calRange", ageRange(_: Int))

    //    val maleHighestbyState = session.sql("select _c3 , SUM(_c9) as TotalMale from (" +
//      "select * from aadhaar where _c7 == 'M') as maleTable group by _c3 order by TotalMale " +
//      "desc limit 3")
//    maleHighestbyState.show()

    val maleHighPercentage = session.sql("select _c3, " +
      "SUM(_c9) as Total, " +
      "SUM(case when _c7 = 'M' then _c9 else 0 end) as TotalMales," +
      "100 * SUM(case when _c7 = 'M' then _c9 else 0 end)/ SUM(_c9) as MaleAadhaarPercentage " +
      "from aadhaar " +
      "group by _c3 " +
      "order by MaleAadhaarPercentage desc " +
      "limit 3")
    maleHighPercentage.show()

    val femaleAadhaarRejected = session.sql("select _c4 as District, " +
      "100 * (SUM(case when _c7 = 'F' then _c10 else 0 end) / " +
      "(SUM(case when _c7 = 'F' then _c10 else 0 end) + " +
      "SUM(case when _c7 = 'F' then _c9 else 0 end))) as RejectedFemalePerc " +
      "from aadhaar " +
      "where _c3 in ('Manipur', 'Arunachal Pradesh', 'Nagaland') " +
      "group by _c4 " +
      "order by RejectedFemalePerc " +
      "desc " +
      "limit 3")
    femaleAadhaarRejected.show()

    val femaleAddhaarGen = session.sql("select _c3 as State, " +
      "SUM(_c9) as TotalAadhaarGenerated, " +
      "SUM(case when _c7 ='F' then _c9 else 0 end) as TotalFemale, " +
      "100 * SUM(case when _c7 = 'F' then _c9 else 0 end) / " +
      "SUM(_c9) as FemaleAadhaarGen " +
      "from aadhaar " +
      "group by _c3 " +
      "order by FemaleAadhaarGen " +
      "desc " +
      "limit 3")
    femaleAddhaarGen.show()

    val maleAadhaarRejected = session.sql("select _c4 as District," +
      "100 * SUM(case when _c7 = 'M' then _c10 else 0 end) / " +
      "(SUM(case when _c7 = 'M' then _c9 else 0 end) + " +
      "SUM(case when _c7 = 'M' then _c10 else 0 end)) as MaleAadhaarRejected " +
      "from aadhaar " +
      "where _c3 in ('Andaman and Nicobar Islands', 'Chhattisgarh', 'Goa') " +
      "group by _c4 " +
      "order by MaleAadhaarRejected " +
      "desc " +
      "limit 3")
    maleAadhaarRejected.show()

    val summaryAcceptancePercentage = session.sql("select calRange(_c8) as AgeGroup, " +
      "100 * SUM(_c9) / (SUM(_c9) + SUM(_c10)) as AcceptancePercentage " +
      "from aadhaar " +
      "group by calRange(_c8) " +
      "order by AgeGroup ")
    summaryAcceptancePercentage.show()

  }

}
