import java.util.{Collections, Properties }
import java.util.concurrent.TimeUnit

import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.concurrent.Executors
import org.apache.flink.streaming.api.datastream.AsyncDataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.async.{ResultFuture, RichAsyncFunction}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}
import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet, SQLException}

import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.streaming.util.serialization.SimpleStringSchema


object AsyncPostgreSQL {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(2)


    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "dggdb01-mon:9092,dggdb02-mon:9092,dggdb03-mon:9092")
    properties.setProperty("zookeeper.connect", "dggdb01-mon:2181,dggdb02-mon:2181,dggdb03-mon:2181")
    properties.setProperty("group.id", "test")

    val kafkaConsumer = new FlinkKafkaConsumer[String]("dbmetric", new SimpleStringSchema(), properties)
    val stream = env.addSource(kafkaConsumer)

    val result = AsyncDataStream.orderedWait(stream, new SampleAsyncFunction, 100000L, TimeUnit.MILLISECONDS, 20).setParallelism(1)
    result.print()


    env.execute("async")
  }

}


  private class SampleAsyncFunction extends RichAsyncFunction[String,String] {
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
            |select name,age from person
            |where name = ?
          """.stripMargin

        stmt = conn.prepareStatement(selectSql)
        stmt.setString(1, "zhe")

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
    def asyncInvoke(s: String, resultFuture: ResultFuture[String]): Unit = {
      var result = "default"
      val rs = stmt.executeQuery()

      while (rs.next()) {
          result  = rs.getString("name")
      }
      resultFuture.complete(Collections.singleton(result))
    }

    @throws[Exception]
    def timeout(input: Nothing, resultFuture: ResultFuture[String]): Unit = {
       resultFuture.complete(Collections.singleton("timeout"))
    }
  }
