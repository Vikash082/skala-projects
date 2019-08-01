package com.vk.settopbox.kpi4

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

import scala.xml.{Node, XML}

object MinMaxDuration {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val session = SparkSession.builder().master("local[2]").getOrCreate()
    val inputData = session.sparkContext.textFile("in/Set_Top_Box_Data.txt")
    val regexStr = "\\^"
    val eId118 = inputData.filter(line => line.split(regexStr)(2).toInt == 118)

    val durationRec = eId118.map(
      line => {
        val splits = line.split(regexStr)
        val xmlStr = splits(4)
        var duration = 0
        val xmlBody = XML.loadString(xmlStr)

        for(nv <- xmlBody.child) {
          val nvRec = XML.loadString(nv.toString())
          val nvKey = nvRec.attribute("n").getOrElse("").toString
          val nvValue = nvRec.attribute("v").getOrElse("").toString

          if (nvKey == "DurationSecs") duration = nvValue.toInt
        }

        duration
      })

    println("Max duration:- " + durationRec.max())
    println("Min duration:- " + durationRec.min())

  }
}
