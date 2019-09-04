package com.aburiaa.ddosdetector


import java.io.{File, PrintWriter}
import java.util.Properties

import org.apache.kafka.clients.consumer.KafkaConsumer

import scala.collection.JavaConverters._
import scala.collection.mutable._
import scala.util.parsing.json.JSON
object Consumer extends App {

  /* Set properties for consumer */
  val props:Properties = new Properties()
  props.put("group.id", "test")
  props.put("bootstrap.servers","localhost:9092")
  props.put("key.deserializer",
    "org.apache.kafka.common.serialization.StringDeserializer")
  props.put("value.deserializer",
    "org.apache.kafka.common.serialization.StringDeserializer")
  props.put("enable.auto.commit", "true")
  props.put("auto.commit.interval.ms", "1000")

  val consumer = new KafkaConsumer(props)
  val topics = List("ddos")
  var result = new scala.collection.mutable.HashMap[String, scala.collection.mutable.Buffer[String]]
  var host = new String
  var time = new String
  val ddosRecords = new ArrayBuffer[String]()
  val writer = new PrintWriter(new File("host-list.txt"))

  try {
  /* Initialize values for use */
    val topics = List("ddos")
    var result = new scala.collection.mutable.HashMap[String, scala.collection.mutable.Buffer[String]]
    var host = new String
    var time = new String
    val ddosRecords = new ArrayBuffer[String]()
    consumer.subscribe(topics.asJava)
    while (true) {
      val records = consumer.poll(10)
      for (record <- records.asScala) {
        val myres = JSON.parseFull(record.value()).get.asInstanceOf[scala.collection.immutable.HashMap[String, String]]
        if (myres.nonEmpty) {
          host = myres.getOrElse("host", "")
          time = myres.getOrElse("time", "")
          if (!result.contains(host))
            result(host) = new ArrayBuffer[String]()
          result(host) += time
          if (!ddosRecords.contains(host) && (result(host).size >= 80)) {
            ddosRecords += host
            writer.write(host + " @ " + time + "\n")
          }
        }
      }
    }
  }catch{
    case e:Exception => e.printStackTrace()
  }finally {
    consumer.close()
    writer.close()
  }
}