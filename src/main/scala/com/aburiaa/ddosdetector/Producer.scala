package com.aburiaa.ddosdetector


import java.util.Properties
import java.util.regex._

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import scala.io.Source
import scala.util.parsing.json.JSONObject
object Producer extends App {

  /*  Create and initialize producer properties  */
  val props = new Properties()
  props.put("bootstrap.servers","localhost:9092")
  props.put("key.serializer",
    "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer",
    "org.apache.kafka.common.serialization.StringSerializer")
  props.put("acks","all")

  /*  Create and initialize values for producer to create and send records  */
  val producer = new KafkaProducer[String, String](props)
  val topic = "ddos"
  val keys = Array("host", "id", "time", "line1", "status", "size", "req_header_ref", "req_header_usr")
  try {
    val fname = System.getProperty("user.dir") + "\\apache-log.txt"
    val regex = """(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})? (\S+) (\S+) (\[.+?\]) \"(.*?)\" (\d{3}) (\S+) \"(.*?)\" \"(.*?)\""""
    val p = Pattern.compile(regex)
    val reader = Source.fromFile(fname)
    for (line <- reader.getLines()) {
      val value = p.matcher(line)
      if (value.matches) {
        val payload = JSONObject(Map[String, String](
          keys(0) -> value.group(1),
          keys(2) -> value.group(4),
          keys(3) -> value.group(5),
          keys(4) -> value.group(6),
          keys(5) -> value.group(7),
          keys(7) -> value.group(9)
        )).toString()
        val record = new ProducerRecord[String, String](topic, "0", payload)
        producer.send(record)
      }
    }
    reader.close()
  }catch{
    case e:Exception => e.printStackTrace()
  }finally {
    producer.close()
  }
}