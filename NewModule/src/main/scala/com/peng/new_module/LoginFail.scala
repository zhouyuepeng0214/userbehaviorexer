package com.peng.new_module

import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

object LoginFail {
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
    env.socketTextStream("localhost", 7777)
      .map(data => {
        val dataArray: Array[String] = data.split(",")
        LoginEvent(dataArray(0).trim().toLong,
          dataArray(1).trim().replace("\"", "").toString(),
          dataArray(2).trim().replace("\"", "").toString(),
          dataArray(3).trim().toLong)
      })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[LoginEvent](Time.seconds(3)) {
        override def extractTimestamp(element: LoginEvent): Long = element.eventTime * 1000
      })
      .keyBy(_.userId)
      .process(new LoginWarning())
      .print()

    env.execute("login fail job")
  }
}

// 输入的登录事件样例类
case class LoginEvent(userId: Long, ip: String, eventType: String, eventTime: Long)

// 输出的报警信息样例类
case class Warning(userId: Long, firstFailTime: Long, lastFailTime: Long, warningMsg: String)


// 自定义处理函数，保存上一次登录失败的事件，并可以注册定时器
class LoginWarning() extends KeyedProcessFunction[Long, LoginEvent, Warning] {
  // 定义保存登录失败事件的状态
  lazy val loginFailState: ListState[LoginEvent] = getRuntimeContext.getListState(new ListStateDescriptor[LoginEvent]("loginfail-state", classOf[LoginEvent]))

  override def processElement(value: LoginEvent, ctx: KeyedProcessFunction[Long, LoginEvent, Warning]#Context, out: Collector[Warning]): Unit = {
    // 判断当前登录状态是否为fail
    //    if (value.eventType == "fail") {
    //      // 把新的失败事件添加到state
    //      loginFailState.add(value)
    //      loginFailState.get().forEach(_)
    //      ctx.timerService().registerEventTimeTimer((value.eventTime + 2) * 1000L)
    //    } else {
    //      // 如果登录成功，清空状态重新开始
    //      loginFailState.clear()
    //    }
    if (value.eventType == "fail") {
      // 先获取之前失败的事件
      val iter = loginFailState.get().iterator()
      // 如果之前已经有失败的事件，就做判断，如果没有就把当前失败事件保存进state
      if (iter.hasNext) {
        val event: LoginEvent = iter.next()
        println(event)
        val firstFailEvent = event
        // 判断两次失败事件间隔小于2秒，输出报警信息
        if (value.eventTime < firstFailEvent.eventTime + 2) {
          out.collect(Warning(value.userId, firstFailEvent.eventTime, value.eventTime, "在2秒内连续两次登录失败。"))
        }
        loginFailState.clear()
        // 把最近一次登录失败保存到state
        loginFailState.add(value)
      } else {
        loginFailState.add(value)
      }
    } else {
      loginFailState.clear()
    }
  }

  //  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, LoginEvent, Warning]#OnTimerContext, out: Collector[Warning]): Unit = {
  //    // 先把状态中的数据取出
  //    val allLoginFailEvents: ListBuffer[LoginEvent] = new ListBuffer[LoginEvent]()
  //
  //    val iter = loginFailState.get().iterator()
  //    while (iter.hasNext) {
  //      val event: LoginEvent = iter.next()
  //      println(event.toString)
  //      allLoginFailEvents += event
  //    }
  //
  //    // 判断登录失败事件个数，如果大于等于2，输出报警信息
  //    if (allLoginFailEvents.length >= 2) {
  //      out.collect(Warning(allLoginFailEvents.head.userId,
  //        allLoginFailEvents.head.eventTime,
  //        allLoginFailEvents.last.eventTime,
  //        "在2秒之内连续登录失败" + allLoginFailEvents.length + "次。"
  //      ))
  //    }
  //    // 清空状态，重新开始计数
  //    loginFailState.clear()
  //  }
}