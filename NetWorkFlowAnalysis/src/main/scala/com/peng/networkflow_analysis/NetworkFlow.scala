package com.peng.networkflow_analysis

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

object NetworkFlow {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

//    val filePath: String = "E:\\ZYP\\code\\UserBehaviorAnalysis\\NetWorkFlowAnalysis\\src\\main\\resources\\apache.log"
//    val stream: DataStream[String] = env.readTextFile(filePath)
    val stream: DataStream[String] = env.socketTextStream("localhost",7777)
    stream.map(data => {
      val dataArray: Array[String] = data.split(" ")

      // todo 获取毫秒时间
      val format: SimpleDateFormat = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss")
      val timestamp: Long = format.parse(dataArray(3).trim()).getTime()
      ApacheLogEvent(dataArray(0).trim(),dataArray(1).trim(),timestamp,dataArray(5).trim(),dataArray(6).trim())
    })
        .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[ApacheLogEvent](Time.seconds(1)) {
          override def extractTimestamp(element: ApacheLogEvent): Long = element.eventTime
        })
        .keyBy(_.url)
        .timeWindow(Time.minutes(10),Time.seconds(5))
        .aggregate(new CountAgg(),new WindowResult())
        .keyBy(_.windowEnd)
        .process(new TopNHotUrls(5))
        .print("network flow test")

    env.execute("network flow test job")
  }
}

case class ApacheLogEvent(ip: String, userId: String, eventTime: Long, method: String, url: String)

case class UrlViewCount(url: String, windowEnd: Long, count: Long)

class CountAgg() extends AggregateFunction[ApacheLogEvent,Long,Long]{
  override def createAccumulator(): Long = 0L

  override def add(value: ApacheLogEvent, accumulator: Long): Long = accumulator + 1

  override def getResult(accumulator: Long): Long = accumulator

  override def merge(a: Long, b: Long): Long = a + b
}

class WindowResult() extends WindowFunction[Long,UrlViewCount,String,TimeWindow]{
  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[UrlViewCount]): Unit = {
    out.collect(UrlViewCount(key,window.getEnd,input.iterator.next()))
  }
}

class TopNHotUrls(nSize: Int) extends KeyedProcessFunction[Long,UrlViewCount,String]{
  private lazy val urlState: ListState[UrlViewCount] = getRuntimeContext().getListState(new ListStateDescriptor[UrlViewCount]("urlState",classOf[UrlViewCount]))

  override def processElement(value: UrlViewCount, ctx: KeyedProcessFunction[Long, UrlViewCount, String]#Context,
                              out: Collector[String]): Unit = {
    urlState.add(value)
    ctx.timerService().registerEventTimeTimer(value.windowEnd + 1L)
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, UrlViewCount, String]#OnTimerContext,
                       out: Collector[String]): Unit = {
    val allUrlViews: ListBuffer[UrlViewCount] = new ListBuffer[UrlViewCount]()
    val iter: util.Iterator[UrlViewCount] = urlState.get().iterator()
    while (iter.hasNext) {
      allUrlViews += iter.next()
    }

    urlState.clear()

    val sortedUrlViews: ListBuffer[UrlViewCount] = allUrlViews.sortWith((data1,data2) => data1.count > data2.count).take(nSize)
    val result : StringBuilder = new StringBuilder()

    result.append("\n=========================================\n")
    result.append("时间：").append(new Timestamp(timestamp - 1)).append("\n")

    for (elem <- sortedUrlViews.indices) {
      val currentUrlView: UrlViewCount = sortedUrlViews(elem)

      result.append("No").append(elem + 1).append(":")
        .append("URL=").append(currentUrlView.url)
        .append("流量：").append(currentUrlView.count).append("\n")
    }
    result.append("=========================================\n")
    Thread.sleep(5000)

    out.collect(result.toString())
  }
}


