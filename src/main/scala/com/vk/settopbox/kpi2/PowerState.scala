package com.vk.settopbox.kpi2

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

import scala.xml.{Node, XML}

object PowerState {

  def attributeValueEquals(value: String)(node: Node): Boolean = {
    node.attributes.exists(_.value.text == value)
  }

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val session = SparkSession.builder().master("local[2]").getOrCreate()
    val inputData = session.sparkContext.textFile("in/Set_Top_Box_Data.txt")
    val regexStr = "\\^"
    val eId101 = inputData.filter(line => line.split(regexStr)(2).toInt == 101)

    val recWithPowerState = eId101.map(
      line => {
        val splits = line.split(regexStr)
        var powerState = ""
        val xmlStr = splits(4)
        val deviceId = splits(5)
        val xmlData = XML.loadString(xmlStr)
        val nvs = xmlData \\ "nv"
        val record = nvs \\ "_" filter attributeValueEquals("PowerState")
        if (record.nonEmpty) {
          val psRec = XML.loadString(record.toString())
          powerState = psRec.attribute("v").getOrElse(0).toString
        }
        (deviceId, powerState)
      })

    val onState = recWithPowerState.filter(rec => rec._2 == "ON").count()
    val offState = recWithPowerState.filter(rec => rec._2 == "OFF").count()

    println("Total ON devices: " + onState)
    println("Total OFF devices: " + offState)
  }

}
