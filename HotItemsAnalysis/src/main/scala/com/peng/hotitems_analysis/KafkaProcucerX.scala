package com.peng.hotitems_analysis

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import scala.io.BufferedSource

object KafkaProcucerX {
  def main(args: Array[String]): Unit = {
    writeToKafka("hotitems")
  }

  def writeToKafka(topic: String): Unit = {
    val properties: Properties = new Properties()
    properties.setProperty("bootstrap.servers", "hadoop102:9092")
    properties.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    properties.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    val producer: KafkaProducer[String, String] = new KafkaProducer[String, String](properties)
    val filePath: String = "E:\\ZYP\\code\\UserBehaviorAnalysis\\HotItemsAnalysis\\src\\main\\resources\\UserBehavior.csv"
    val bufferedSource: BufferedSource = io.Source.fromFile(filePath)

    for (elem <- bufferedSource.getLines()) {
      val record: ProducerRecord[String, String] = new ProducerRecord[String,String](topic,elem)
      producer.send(record)
    }
    producer.close()
  }

}


