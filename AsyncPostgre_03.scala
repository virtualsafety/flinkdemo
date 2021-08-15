import java.util.{Calendar, Collections, Properties}
import java.util.concurrent.TimeUnit

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.AsyncDataStream
import org.apache.flink.streaming.api.scala.async.{AsyncFunction, ResultFuture}
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.concurrent.Executors
//import org.apache.flink.streaming.api.functions.async.{ResultFuture, RichAsyncFunction}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.common.typeinfo.TypeHint

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}
import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet, SQLException}

import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows
import org.apache.flink.streaming.api.windowing.evictors.CountEvictor
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.streaming.util.serialization.SimpleStringSchema
import org.apache.flink.util.Collector
import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods.{compact, parse, render}



object AsyncPostgre{
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(2)


    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "xxxxxx01-mon:9092,xxxxxx02-mon:9092,xxxxxx03-mon:9092")
    properties.setProperty("zookeeper.connect", "xxxxxx01-mon:2181,xxxxxx02-mon:2181,xxxxxx03-mon:2181")
    properties.setProperty("group.id", "test")
    properties.setProperty("auto.offset.reset", "latest")
    properties.setProperty("enable.auto.commit", "true")
    properties.setProperty("auto.commit.interval.ms", "10")

    val kafkaConsumer = new FlinkKafkaConsumer[String]("osmonitor", new SimpleStringSchema(), properties)
    val stream = env.addSource(kafkaConsumer).map(new CpuMap)

    val result = AsyncDataStream.orderedWait(stream, new SampleAsyncFunction(), 100000L, TimeUnit.MILLISECONDS, 20)
        .setParallelism(1)
        .keyBy(in => (in.oracle_sid,in.hostname))
        .window(GlobalWindows.create())
        .evictor(CountEvictor.of(CpuWindow.WindowSize)) //windows size
        .trigger(CountTrigger.of(1)) //slide size
        .apply(new CpuCheck())
        .map(new  CpuTupleToJson)
        .print()

       env.execute("async")
  }
}


private class SampleAsyncFunction extends  AsyncFunction[CpuStatExt,CpuStatExt] {
  implicit lazy val executor: ExecutionContext = ExecutionContext.fromExecutor(Executors.directExecutor())

  private var conn: Connection = null
  private var stmt: PreparedStatement = null

  @throws[SQLException]
  def asyncInvoke(s:  CpuStatExt, resultFuture: ResultFuture[ CpuStatExt]): Unit = {

    try {
      Class.forName("org.postgresql.Driver")
      conn = DriverManager.getConnection("jdbc:postgresql://x.x.x.x:x/xdbadb", "xxxx", "xxxx")

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

    var result =  85
    stmt.setString(1, s.hostname)
    val rs = stmt.executeQuery()

    while (rs.next()) {
      result  = rs.getInt("threshold")
    }

    //println(s"get from database :  $result")

    conn.close()
    stmt.close()

    s.threshold =  result
    resultFuture.complete(Iterable(s))
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


class  CpuCheck extends WindowFunction[CpuStatExt, (String, String, String, String, Int, Int), (String, String), GlobalWindow] {
  def toInt(s: String): Option[Int] = {
    try {
      Some(Integer.parseInt(s.trim))
    } catch {
      case e: Exception => None
    }
  }

  def apply(key: (String, String), window: GlobalWindow, input: Iterable[CpuStatExt], out: Collector[(String, String, String, String, Int, Int)]): Unit = {
    var Tthreshold = input.last.threshold

    val last_cpu_usage = toInt(input.last.metric_value).getOrElse(120)

    var count_3 = 0 //level_3 counter
    var metric_value = 0

    val now = Calendar.getInstance()
    val currentHour = now.get(Calendar.HOUR_OF_DAY)
    if ( currentHour >= 20  ||  currentHour < 7)  {
      Tthreshold = Tthreshold + 1
    }

    for (in <- input) {
      //if cpu greater than 100
      metric_value = toInt(in.metric_value).getOrElse(120)
      if (metric_value >= Tthreshold) {
        count_3 = count_3 + 1
      }
    }

    //if level_3 counter  equal WindowSize
    if (count_3 == CpuWindow.WindowSize) {
      out.collect((key._1, key._2, metric_value.toString(), input.map(_.metric_tm).last, last_cpu_usage, 3))
    }
  }
}

class  CpuTupleToJson extends MapFunction[(String, String, String, String, Int, Int), String] {
  def map(tuple: (String, String, String, String, Int, Int)): String = {

    val ext_col1 =  tuple._3
    val ext_col2 = (tuple._5 - tuple._3.toFloat).toString

    val oscpuTuple = ("ORACLE_SID" -> tuple._1) ~
      ("HOSTNAME" -> tuple._2) ~
      ("SAMPLE_TYPE" -> "Os_Cpu") ~
      ("SAMPLE_VALUE" -> tuple._3) ~
      ("EXT_COL1" -> "------") ~
      ("EXT_COL2" -> "------") ~
      ("SAMPLE_TM" -> tuple._4)
    compact(render(oscpuTuple)) //tuple to json
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

object  CpuWindow {
  final val  WindowSize = 3
}
