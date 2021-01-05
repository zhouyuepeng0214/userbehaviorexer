package com.peng.ordertimeout_detect

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

object OrderTimeoutWithStatePro {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    println("获取数据")
    //    val orderEventStream: KeyedStream[OrderEvent, Int] = env.fromCollection(List(
    //      OrderEvent(1, "create", 1558430842),
    //      OrderEvent(2, "create", 1558430843),
    //      OrderEvent(2, "other", 1558430845),
    //      OrderEvent(2, "pay", 1558430850),
    //      OrderEvent(1, "pay", 1558431920)))
    val orderEventStream: KeyedStream[OrderEvent, Long] = env.socketTextStream("localhost", 7777)
      .map(data => {
        val dataArray: Array[String] = data.split(",")
        OrderEvent(dataArray(0).trim.toLong, dataArray(1).trim, dataArray(2).trim.toLong)
      })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[OrderEvent](Time.seconds(8)) {
        override def extractTimestamp(element: OrderEvent): Long = {
          element.eventTime * 1000L
        }
      })
      .keyBy(_.orderId)

    val timeoutWarningStream = orderEventStream.process(new OrderPayMatch())

    timeoutWarningStream.print("warning")

    env.execute("order timeout with state job")
  }
}

class OrderPayMatch() extends KeyedProcessFunction[Long, OrderEvent, OrderResult] {
  override def processElement(value: OrderEvent, ctx: KeyedProcessFunction[Long, OrderEvent, OrderResult]#Context,
                              out: Collector[OrderResult]): Unit = {

  }
}

