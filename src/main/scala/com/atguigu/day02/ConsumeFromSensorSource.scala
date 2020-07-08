package com.atguigu.day02

import org.apache.flink.streaming.api.scala._

object ConsumeFromSensorSource {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // 调用addSource方法
    val sensorReadingStream: DataStream[SensorReading] = env.addSource(new SensorSource)

    sensorReadingStream.print()

    env.execute()
  }

}
