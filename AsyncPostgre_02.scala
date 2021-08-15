package com.huawei.flink

import java.util.{Collections, Properties}
import java.util.concurrent.TimeUnit

import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.concurrent.Executors
import org.apache.flink.streaming.api.datastream.AsyncDataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.async.{ResultFuture, RichAsyncFunction}
import org.apache.flink.api.scala._
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.common.typeinfo.TypeHint

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}
import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet, SQLException}

import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.streaming.util.serialization.SimpleStringSchema
import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods.parse



object AsyncPostgre{
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(2)


    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "xxxxx01-mon:9092,xxxxx02-mon:9092,xxxxx03-mon:9092")
    properties.setProperty("zookeeper.connect", "xxxxx01-mon:2181,xxxxx02-mon:2181,xxxxx03-mon:2181")
    properties.setProperty("group.id", "test")

    val kafkaConsumer = new FlinkKafkaConsumer[String]("osmonitor", new SimpleStringSchema(), properties)
    val stream = env.addSource(kafkaConsumer).map(new CpuMap)

    val result = AsyncDataStream.orderedWait(stream, new SampleAsyncFunction, 100000L, TimeUnit.MILLISECONDS, 20).setParallelism(1)
    result.returns(TypeInformation.of(new TypeHint[CpuStatExt]{})).print()



    env.execute("async")
  }



}


private class SampleAsyncFunction extends RichAsyncFunction[CpuStatExt,CpuStatExt] {
  implicit lazy val executor: ExecutionContext = ExecutionContext.fromExecutor(Executors.directExecutor())

  private var conn: Connection = null
  private var stmt: PreparedStatement = null

  @throws[Exception]
  override def open(conf: Configuration ): Unit = {
    super.open(conf)
    try {
      Class.forName("org.postgresql.Driver")
      conn = DriverManager.getConnection("jdbc:postgresql://x.x.x.x:5432/xxxxdb", "xxxx", "xxxx")

      val selectSql =
        """
          |select hostname,threshold from host_threshold
          |where hostname = ?
        """.stripMargin

      stmt = conn.prepareStatement(selectSql)


    } catch {
      case e: Exception =>
        e.printStackTrace()
    }
  }

  @throws[Exception]
  override  def close(): Unit = {
    super.close()
    conn.close()
    stmt.close()
  }

  @throws[SQLException]
  def asyncInvoke(s:  CpuStatExt, resultFuture: ResultFuture[ CpuStatExt]): Unit = {
    var result =  85


    stmt.setString(1, s.hostname)
    val rs = stmt.executeQuery()

    while (rs.next()) {
      result  = rs.getInt("threshold")
    }
    //println(s"get from database :  $result")

    s.threshold =  result
    resultFuture.complete(Collections.singleton(s))
  }

  @throws[Exception]
  def timeout(input: Nothing, resultFuture: ResultFuture[String]): Unit = {
    resultFuture.complete(Collections.singleton("timeout"))
  }
}


class  CpuMap extends MapFunction[String,  CpuStatExt] {
  def map(str: String): CpuStatExt = {
    try {
      implicit val formats = DefaultFormats;

      var json = parse(str)
      json = json merge JObject("threshold" -> JInt(85))
      val cpustat = json.extract[ CpuStatExt] //parse json to case class
      cpustat
    } catch {
      case _: Throwable =>  CpuStatExt("flink", "xxxx-mon", "cpu_usage", "10",  "201701010000", 85  )
    }
  }
}

case class CpuStat(oracle_sid: String,
                   hostname: String,
                   metric_type: String,
                   metric_value: String,
                   metric_tm: String)

case class CpuStatExt(oracle_sid: String,
                      hostname: String,
                      metric_type: String,
                      metric_value: String,
                      metric_tm: String,
                      var   threshold:   Int   )
