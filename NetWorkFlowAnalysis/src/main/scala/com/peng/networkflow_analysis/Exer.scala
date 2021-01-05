package com.peng.networkflow_analysis

import java.text.SimpleDateFormat
import java.util.Date

object Exer {
  def main(args: Array[String]): Unit = {
    val format: SimpleDateFormat = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss")
    val date: Date = format.parse("17/06/2019 10:05:03")
    println(date)
    println(date.getTime)
  }

}
