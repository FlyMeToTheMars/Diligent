package com.hugh.scala.flink.cep.source

import java.sql.{Connection, PreparedStatement}
import java.util.ResourceBundle

import com.hugh.scala.flink.cep.jdbc.DBConnectionPool
import com.hugh.scala.flink.cep.pojo.RuleInFo
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}

/**
 * @Author Fly.Hugh
 * @Date 2020/4/8 11:42
 * @Version 1.0
 **/
class JdbcReader extends RichSourceFunction[RuleInFo]{

  private var connection:Connection = null
  private var ps:PreparedStatement = null
  private var runningFlag:Boolean = true

  override def run(ctx: SourceFunction.SourceContext[RuleInFo]): Unit = {
    while(runningFlag) {
      val resultSet = ps.executeQuery()
      while(resultSet.next()) {
        val ruleInFo = RuleInFo(resultSet.getString("id"),resultSet.getDouble("threshold"))
        ctx.collect(ruleInFo)
      }

      //Refresh RuleInfo per 2min
      Thread.sleep(1000L*60L*2L)
    }
  }

  override def cancel(): Unit = {
    super.close()
    if(connection!=null) {
      connection.close()
    }
    if(ps!=null) {
      ps.close()
    }
    runningFlag = false
  }

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    connection = DBConnectionPool.getConn()
    ps = connection.prepareStatement("select id,threshold from event_mapping")
  }
}
