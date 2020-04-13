package test

import com.hugh.scala.flink.cep.source.{JdbcReader, SimulatedSource}
import org.apache.flink.streaming.api.scala._

/**
 * @Author Fly.Hugh
 * @Date 2020/4/8 11:37
 * @Version 1.0
 **/
object test {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val value = env.addSource(new JdbcReader)
    value.print()

    env.execute("test Source")
  }
}