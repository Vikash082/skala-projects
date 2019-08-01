package com.vk.settopbox.kpi7

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

import scala.xml.XML

object DurationByProgramId {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val session = SparkSession.builder().master("local[2]").getOrCreate()
    val inputData = session.sparkContext.textFile("in/Set_Top_Box_Data.txt")
    val regexStr = "\\^"
    val eId115or118 = inputData.filter(line => {
      val eventId = line.split(regexStr)(2).toInt
      eventId == 115 || eventId == 118
    })
    /**
    Below filter is necessary as there are also invalid HTML records in the Input. We will encounter
      below exception if do not check explicitly.


    org.xml.sax.SAXParseException; lineNumber: 1; columnNumber: 1; Content is not allowed in prolog.
    at com.sun.org.apache.xerces.internal.util.ErrorHandlerWrapper.createSAXParseException(ErrorHandlerWrapper.java:203)
    at com.sun.org.apache.xerces.internal.util.ErrorHandlerWrapper.fatalError(ErrorHandlerWrapper.java:177)
    at com.sun.org.apache.xerces.internal.impl.XMLErrorReporter.reportError(XMLErrorReporter.java:400)
    at com.sun.org.apache.xerces.internal.impl.XMLErrorReporter.reportError(XMLErrorReporter.java:327)
    at com.sun.org.apache.xerces.internal.impl.XMLScanner.reportFatalError(XMLScanner.java:1472)
    at com.sun.org.apache.xerces.internal.impl.XMLDocumentScannerImpl$PrologDriver.next(XMLDocumentScannerImpl.java:994)
    at com.sun.org.apache.xerces.internal.impl.XMLDocumentScannerImpl.next(XMLDocumentScannerImpl.java:602)
    at com.sun.org.apache.xerces.internal.impl.XMLDocumentFragmentScannerImpl.scanDocument(XMLDocumentFragmentScannerImpl.java:505)
    at com.sun.org.apache.xerces.internal.parsers.XML11Configuration.parse(XML11Configuration.java:842)
    at com.sun.org.apache.xerces.internal.parsers.XML11Configuration.parse(XML11Configuration.java:771)
    at com.sun.org.apache.xerces.internal.parsers.XMLParser.parse(XMLParser.java:141)
    at com.sun.org.apache.xerces.internal.parsers.AbstractSAXParser.parse(AbstractSAXParser.java:1213)
    at com.sun.org.apache.xerces.internal.jaxp.SAXParserImpl$JAXPSAXParser.parse(SAXParserImpl.java:643)
    at com.sun.org.apache.xerces.internal.jaxp.SAXParserImpl.parse(SAXParserImpl.java:327)
    at scala.xml.factory.XMLLoader$class.loadXML(XMLLoader.scala:41)
    at scala.xml.XML$.loadXML(XML.scala:60)
    at scala.xml.factory.XMLLoader$class.loadString(XMLLoader.scala:60)
    at scala.xml.XML$.loadString(XML.scala:60)
      **/
      .filter { line => {
        val tokens = line.split(regexStr)
        val body = tokens(4)
        body.contains("<d>")
      }}

    val records = eId115or118.map(
      line => {
        val splits = line.split(regexStr)
        val xmlStr = splits(4)
        var duration = 0
        var programId = ""
        val xmlBody = XML.loadString(xmlStr.toString)

        for (nv <- xmlBody.child) {
          val nvRec = XML.loadString(nv.toString())
          val nvKey = nvRec.attribute("n").getOrElse("").toString
          val nvVal = nvRec.attribute("v").getOrElse("").toString

          if (nvKey == "ProgramId") programId = nvVal
          if (nvKey == "DurationSecs" && nvVal != "") duration = nvVal.toInt
        }
        (programId, duration)
      })
      .groupByKey()

    records.saveAsTextFile("out/kpi7")
  }

}
