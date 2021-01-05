package com.peng.ordertimeout_detect

import java.util

import org.apache.flink.cep.{PatternSelectFunction, PatternTimeoutFunction}
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object OrderTimeout {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    println("获取数据")
    val orderEventStream: KeyedStream[OrderEvent, Long] = env.fromCollection(List(
      OrderEvent(1, "create", 1558430842),
      OrderEvent(2, "create", 1558430843),
      OrderEvent(2, "other", 1558430845),
      OrderEvent(2, "pay", 1558430850),
      OrderEvent(1, "pay", 1558431920)))
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[OrderEvent](Time.seconds(8)) {
        override def extractTimestamp(element: OrderEvent): Long = {
          element.eventTime * 1000L
        }
      })
      .keyBy(_.orderId)
    val orderPayPattern: Pattern[OrderEvent, OrderEvent] = Pattern.begin[OrderEvent]("start")
      .where(_.eventType == "create")
      .followedBy("follow")
      .where(_.eventType == "pay")
      .within(Time.minutes(10))
    val patternStream: PatternStream[OrderEvent] = CEP.pattern(orderEventStream,orderPayPattern)

    val orderTimeoutOutputTag: OutputTag[OrderResult] = new OutputTag[OrderResult]("orderTimeout")

    val complexResultStream: DataStream[OrderResult] = patternStream.select(orderTimeoutOutputTag,
      new OrderTimeoutSelect(),
      new OrderPaySelect())

    complexResultStream.print("payed order")
    complexResultStream.getSideOutput(orderTimeoutOutputTag).print("timeout order")

    env.execute("order timeout warning job")
  }

}

case class OrderEvent(orderId: Long, eventType: String, eventTime: Long)

case class OrderResult(orderId: Long, resultMsg: String)

class OrderTimeoutSelect() extends PatternTimeoutFunction[OrderEvent,OrderResult]{
  override def timeout(map: util.Map[String, util.List[OrderEvent]], l: Long): OrderResult = {
    val timeoutOrderEvent: OrderEvent = map.get("start").iterator().next()
    OrderResult(timeoutOrderEvent.orderId,"timeout")
  }
}

class OrderPaySelect() extends PatternSelectFunction[OrderEvent,OrderResult]{
  override def select(map: util.Map[String, util.List[OrderEvent]]): OrderResult = {
    val payOrderEvent: OrderEvent = map.get("follow").iterator().next()
    OrderResult(payOrderEvent.orderId,"payed successfully")
  }
}
