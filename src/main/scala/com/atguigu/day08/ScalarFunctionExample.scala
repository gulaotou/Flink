package com.atguigu.day08

import com.atguigu.day03.SensorSource
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.scala._
import org.apache.flink.table.functions.ScalarFunction
import org.apache.flink.types.Row

object ScalarFunctionExample {
  case class test(id: String,
                  ts: Long,
                  age:Double)
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream = env
      .fromElements(
        test("2",20000L,88.88),
        test("3",30000L,99.99),
        test("4",40000L,66.66)
      )

    val settings = EnvironmentSettings
      .newInstance()
      .useBlinkPlanner()
      .inStreamingMode()
      .build()

    val tEnv = StreamTableEnvironment.create(env, settings)

    val hashCode = new HashCode(10)

    // table写法
    val table = tEnv.fromDataStream(stream)

    table
      .select('id,'ts, hashCode('id))
      .toAppendStream[Row]
        .print()

   /* // sql 写法
    tEnv.registerFunction("hashCode", hashCode)

    tEnv.createTemporaryView("sensor", table)

    tEnv
      .sqlQuery("SELECT id, hashCode(id) FROM sensor")
      .toAppendStream[Row]
      .print()*/

    env.execute()
  }

  class HashCode(factor: Int) extends ScalarFunction {
    def eval(s: String): Int = {
      2* factor
    }
  }
}