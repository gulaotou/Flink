package Flink

import com.atguigu.day01.WordCount.WordWithCount
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object Test {

  case class WordWithCount(word: String, count: Int)

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)

    val stream=env.socketTextStream("hadoop102",9988,'\n')
    stream.flatMap(line=>line.split("\\s"))
      .map(w=>WordWithCount(w,1))
      .keyBy(0).timeWindow(Time.seconds(5))
      .sum(1)
    stream.print()

    env.execute()
  }
}
