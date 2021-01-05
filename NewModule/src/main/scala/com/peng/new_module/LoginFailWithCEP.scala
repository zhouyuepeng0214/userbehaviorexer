package com.peng.new_module

import java.util

import org.apache.flink.cep.PatternSelectFunction
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object LoginFailWithCEP {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    println("获取数据")
    //    env.fromCollection(List(
    //      LoginEvent(1, "192.168.0.1", "fail", 1558430842),
    //      //      LoginEvent(1, "192.168.0.3", "success", 1558430845),
    //      LoginEvent(1, "192.168.0.2", "fail", 1558430843),
    //      LoginEvent(1, "192.168.0.3", "fail", 1558430844),
    //      LoginEvent(2, "192.168.10.10", "success", 1558430845),
    //      LoginEvent(2, "192.168.10.10", "success", 1558430846)
    //    ))

    val loginEventStream: KeyedStream[LoginEvent, Long] = env.socketTextStream("localhost", 7777)
      .map(data => {
        val dataArray: Array[String] = data.split(",")
        LoginEvent(dataArray(0).trim().toLong,
          dataArray(1).trim().replace("\"", "").toString(),
          dataArray(2).trim().replace("\"", "").toString(),
          dataArray(3).trim().toLong)
      })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[LoginEvent](Time.seconds(0)) {
        override def extractTimestamp(element: LoginEvent): Long = element.eventTime * 1000
      })
      .keyBy(_.userId)

    loginEventStream.print("input")

    // 2. 定义匹配的模式
    val loginFailPattern: Pattern[LoginEvent, LoginEvent] = Pattern.begin[LoginEvent]("begin")
      .where(_.eventType == "fail")
      .next("next")
      .where(_.eventType == "fail")
      .within(Time.seconds(10))

    val patternStream: PatternStream[LoginEvent] = CEP.pattern(loginEventStream,loginFailPattern)

    val loginFailDataStream: DataStream[Warning] = patternStream.select(new LoginFailMatch())
    loginFailDataStream.print("warning")

    env.execute("login fail with cep job")
  }
}

class LoginFailMatch() extends PatternSelectFunction[LoginEvent,Warning]{
  override def select(map: util.Map[String, util.List[LoginEvent]]): Warning = {
    val firstFail: LoginEvent = map.get("begin").iterator().next()
    val secondFail: LoginEvent = map.get("next").iterator().next()
    Warning(firstFail.userId,firstFail.eventTime,secondFail.eventTime,"最近2次登陆连续2次登陆失败")
  }
}
