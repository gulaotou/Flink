package com.atguigu.day07

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object OrderTimeoutWithoutCEP {
  case class OrderEvent(orderId: String, eventType: String, eventTime: Long)
  def main(args: Array[String]): Unit = {
    val env =StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val stream = env.fromElements(
      OrderEvent("order_1", "create", 2000L),
      OrderEvent("order_2", "create", 3000L),
      OrderEvent("order_2", "pay", 4000L)
    ).assignAscendingTimestamps(_.eventTime)
      .keyBy(_.orderId)
      .process(new MatchFunction)

    stream.print()

    env.execute()
  }

  class MatchFunction extends KeyedProcessFunction[String,OrderEvent,String]{
    var orderState: ValueState[OrderEvent] = _
    override def open(parameters: Configuration): Unit = {
      orderState=getRuntimeContext.getState(
        new ValueStateDescriptor[OrderEvent]("saved order",Types.of[OrderEvent])
      )
    }

    override def processElement(i: OrderEvent, context: KeyedProcessFunction[String, OrderEvent, String]#Context, collector: Collector[String]): Unit = {
      if(i.eventType.equals("create")){
        if (orderState.value()==null){
          orderState.update(i)
          context.timerService().registerEventTimeTimer(i.eventTime+5000L)
        }
      }else{
        // 保存的是pay事件
        collector.collect("已经支付的订单是：" + i.orderId)
        orderState.update(i)
      }
    }

    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, OrderEvent, String]#OnTimerContext, out: Collector[String]): Unit = {
      val savedOrder = orderState.value()

      if (savedOrder != null && savedOrder.eventType.equals("create")) {
        out.collect("超时订单是 " + savedOrder.orderId)
      }
      orderState.clear()
    }
  }
}
