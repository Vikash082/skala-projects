package com.vk.settopbox.kpi6

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

import scala.xml.XML

object ButtonNames {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val session = SparkSession.builder().master("local[2]").getOrCreate()
    val inputData = session.sparkContext.textFile("in/Set_Top_Box_Data.txt")
    val regexStr = "\\^"
    val eId107 = inputData.filter(line => line.split(regexStr)(2).toInt == 107)

    val records = eId107.map(
      line => {
        val splits = line.split(regexStr)
        val xmlStr = splits(4)
        val deviceId = splits(5)
        var buttonName = ""
        val xmlBody = XML.loadString(xmlStr)

        for(nv <- xmlBody.child) {
          val nvRec = XML.loadString(nv.toString())
          val nvKey = nvRec.attribute("n").getOrElse("").toString
          val nvVal = nvRec.attribute("v").getOrElse("").toString

          if (nvKey == "ButtonName") buttonName = nvVal
        }

        (deviceId, buttonName)
      })
      .groupByKey()

    records.saveAsTextFile("out/kpi6")

  }

}
