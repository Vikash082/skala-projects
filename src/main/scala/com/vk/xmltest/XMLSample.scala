package com.vk.xmltest

import scala.xml.{Node, XML}

object XMLSample {

  def attributeValueEquals(value: String)(node: Node): Boolean = {
    node.attributes.exists(_.value.text == value)
  }

  def main(args: Array[String]): Unit = {

    val data = "11001^1^100^2015-06-05 22:35:21.543^<d><nv n=\"ExtStationID\" v=\"Station/FYI Television, Inc./25102\" /><nv n=\"MediaDesc\" v=\"19b8f4c0-92ce-44a7-a403-df4ee413aca9\" /><nv n=\"ChannelNumber\" v=\"1366\" /><nv n=\"Duration\" v=\"24375\" /><nv n=\"IsTunedToService\" v=\"True\" /><nv n=\"StreamSelection\" v=\"FULLSCREEN_PRIMARY\" /><nv n=\"ChannelType\" v=\"LiveTVMediaChannel\" /><nv n=\"TuneID\" v=\"636007629215440000\" /></d>^0122648d-4352-4eec-9327-effae0c34ef2^2016060601"
    val regexStr = "\\^"
    val splits = data.split(regexStr)
    val xmlStr = splits(4)
    val deviceId = splits(5)
    //  println(xmlStr)
    val xmlData = XML.loadString(xmlStr)
    //  println(xmlData)
    val res = xmlData \\ "nv"
    println(res)
    val finalRes = res \\ "_" filter attributeValueEquals("Duration")
    println(finalRes)
    val n1 = XML.loadString(finalRes.toString())
    println(n1)
    println("n: " + n1.attribute("n"))
    println("v: " + n1.attribute("v"))

    println("Sum: " + (n1.attribute("v").getOrElse(0).toString.toInt + 100))
  }
}
