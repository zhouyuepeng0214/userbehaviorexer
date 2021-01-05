package com.peng.hotitems_analysis


import java.sql.Timestamp
import java.util.Properties

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

object HotItems {
  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val filePath: String = "E:\\ZYP\\code\\UserBehaviorAnalysis\\HotItemsAnalysis\\src\\main\\resources\\UserBehavior.csv"
    val stream: DataStream[String] = env.readTextFile(filePath)
//    val properties: Properties = new Properties()
//    properties.setProperty("bootstrap.servers", "hadoop110:9092")
//    properties.setProperty("group.id", "consumer-group")
//    properties.setProperty("key.deserializer",
//      "org.apache.kafka.common.serialization.StringDeserializer")
//    properties.setProperty("value.deserializer",
//      "org.apache.kafka.common.serialization.StringDeserializer")
//    properties.setProperty("auto.offset.reset", "latest")
//
//    val kafkaStream: DataStream[String] = env.addSource(new FlinkKafkaConsumer[String]("hotitems",new SimpleStringSchema(),properties))

    stream.map(data => {
      val dataArray: Array[String] = data.split(",")
      UserBehavior(dataArray(0).trim.toLong, dataArray(1).trim.toLong, dataArray(2).trim.toInt, dataArray(3).trim, dataArray(4).trim.toLong)
    })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[UserBehavior](Time.seconds(1)) {
        override def extractTimestamp(element: UserBehavior): Long = element.timestamp * 1000
      })
      .filter(_.behavior == "pv")
      .keyBy(_.itemId)
      //滑动窗口
      .timeWindow(Time.hours(1), Time.minutes(5))
      .aggregate(new CountAgg(), new WindowResult())
      .keyBy(_.windowEnd)
      .process(new TopNHotItems(3))
      .print("top3 items")

    env.execute("hot items job")
  }

}

case class UserBehavior(userId: Long, itemId: Long, categoryId: Int, behavior: String, timestamp: Long)

case class ItemViewCount(itemId: Long, windowEnd: Long, count: Long)

class CountAgg() extends AggregateFunction[UserBehavior, Long, Long] {
  override def createAccumulator(): Long = {
    0L
  }

  override def add(value: UserBehavior, accumulator: Long): Long = {
    accumulator + 1L
  }

  override def getResult(accumulator: Long): Long = accumulator

  override def merge(a: Long, b: Long): Long = a + b
}

class WindowResult() extends WindowFunction[Long, ItemViewCount, Long, TimeWindow] {
  override def apply(key: Long, window: TimeWindow, input: Iterable[Long], out: Collector[ItemViewCount]): Unit = {
    val itemId: Long = key
    val windowEnd: Long = window.getEnd()
    val count: Long = input.iterator.next()
    out.collect(ItemViewCount(itemId, windowEnd, count))
  }
}

class AverageAgg() extends AggregateFunction[UserBehavior, (Long, Int), Double] {
  override def createAccumulator(): (Long, Int) = (0L, 0)

  override def add(value: UserBehavior, accumulator: (Long, Int)): (Long, Int) = (accumulator._1 + value.timestamp, accumulator._2 + 1)

  override def getResult(accumulator: (Long, Int)): Double = accumulator._1 / accumulator._2

  override def merge(a: (Long, Int), b: (Long, Int)): (Long, Int) = (a._1 + b._1, a._2 + b._2)
}

class TopNHotItems(nSize: Int) extends KeyedProcessFunction[Long, ItemViewCount, String] {

  private var itemState: ListState[ItemViewCount] = _

  override def open(parameters: Configuration): Unit = {
    itemState = getRuntimeContext().getListState(new ListStateDescriptor[ItemViewCount]("itemstate", classOf[ItemViewCount]))
  }

  override def processElement(value: ItemViewCount, ctx: KeyedProcessFunction[Long, ItemViewCount, String]#Context,
                              out: Collector[String]): Unit = {
    itemState.add(value)
    ctx.timerService().registerEventTimeTimer(value.windowEnd + 100)
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, ItemViewCount, String]#OnTimerContext,
                       out: Collector[String]): Unit = {

    val allItems: ListBuffer[ItemViewCount] = new ListBuffer[ItemViewCount]()

    import scala.collection.JavaConversions._
    for (elem <- itemState.get()) {
      allItems += elem
    }
    itemState.clear()
    // todo 从大到小排序
    val sortedItems: ListBuffer[ItemViewCount] = allItems.sortBy(_.count)(Ordering.Long.reverse).take(nSize)

    //格式化topn
    val result: StringBuilder = new StringBuilder()
    result.append("\n===========================\n")
    result.append("时间:").append(new Timestamp(timestamp - 100)).append("\n")

    for (i <- sortedItems.indices) {
      val currentItem: ItemViewCount = sortedItems(i)
      result.append("No").append(i + 1).append(":")
        .append("商品ID：").append(currentItem.itemId)
        .append("浏览量：").append(currentItem.count)
        .append("\n")
    }
    result.append("===============================================")

    Thread.sleep(1000)
    out.collect(result.toString())
  }
}

