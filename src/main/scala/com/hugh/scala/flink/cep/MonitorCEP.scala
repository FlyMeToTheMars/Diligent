package com.hugh.scala.flink.cep

import java.util
import java.util.{Map, Properties}

import com.alibaba.fastjson.JSON
import com.hugh.scala.flink.cep.pojo.{DynamicMessage, RuleInFo}
import com.hugh.scala.flink.cep.source.{JdbcReader, SimulatedSource}
import com.hugh.scala.flink.cep.pojo.RuleInFo
import org.apache.flink.api.common.state.{BroadcastState, MapStateDescriptor}
import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeHint, TypeInformation}
import org.apache.flink.streaming.api.datastream.BroadcastStream
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
 * @Author Fly.Hugh
 * @Date 2020/4/8 13:45
 * @Version 1.0
 **/
object MonitorCEP {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //This can be used to create state where the type is a map that can be updated and iterated over.
    val descriptor:MapStateDescriptor[String,RuleInFo] =
      new MapStateDescriptor("Rule_InFo",BasicTypeInfo.STRING_TYPE_INFO,TypeInformation.of(new TypeHint[RuleInFo] {}))

    val ruleStream: BroadcastStream[RuleInFo] = env.addSource(new JdbcReader).broadcast(descriptor)



    val fitStream: DataStream[String] = env.addSource(new SimulatedSource)
      .connect(ruleStream)
      .process(new BroadcastProcessFunction[DynamicMessage, RuleInFo, String] {
        override def processBroadcastElement(value: RuleInFo, ctx: BroadcastProcessFunction[DynamicMessage, RuleInFo, String]#Context, out: Collector[String]): Unit = {
          //use created descriptor to create state
          val state: BroadcastState[String, RuleInFo] = ctx.getBroadcastState(descriptor)
          //get rule id
          val id: String = value.id
          state.put(id, value)
        }

        //compile the id in two stream,if so,export String Stream
        override def processElement(value: DynamicMessage, ctx: BroadcastProcessFunction[DynamicMessage, RuleInFo, String]#ReadOnlyContext, out: Collector[String]): Unit = {
          val state= ctx.getBroadcastState(descriptor).immutableEntries().iterator()
          while (state.hasNext) {
            val mapEntry: Map.Entry[String, RuleInFo] = state.next()
            //mapEntry.getValue is RuleInFo
            if (value.id == mapEntry.getValue.id) {
              out.collect(value.toString + mapEntry.getValue.threshold)
            }
          }
        }
      })

    fitStream.print()

    env.execute("fit Fun")
  }
}