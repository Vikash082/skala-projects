package com.vk.settopbox.kpi5

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

import scala.xml.XML

object BadBlock {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val session = SparkSession.builder().master("local[2]").getOrCreate()
    val inputData = session.sparkContext.textFile("in/Set_Top_Box_Data.txt")
    val regexStr = "\\^"
    val eId0 = inputData.filter(line => line.split(regexStr)(2).toInt == 0)

    val badBlockRecs = eId0.map(
      line => {
        val splits = line.split(regexStr)
        val xmlStr = splits(4)
        var badBlocks = ""
        var badValue = 0
        val xmlBody = XML.loadString(xmlStr)

        for(nv <- xmlBody.child) {
          val nvRec = XML.loadString(nv.toString())
          val nvKey = nvRec.attribute("n").getOrElse("").toString
          val nvValue = nvRec.attribute("v").getOrElse("").toString

          if (nvKey == "BadBlocks") {
            badBlocks = nvKey
            badValue = nvValue.toInt
          }
        }
        (badBlocks, badValue)
      })
      .filter(rec => rec._1 == "BadBlocks")
      .count()

    println("Total BadBlocks rec :- " + badBlockRecs)
  }

}
