import java.util.Collections
import java.util.concurrent.{TimeUnit}

import org.apache.flink.runtime.concurrent.Executors
import org.apache.flink.streaming.api.datastream.AsyncDataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.async.{AsyncFunction, ResultFuture}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}


object AsyncTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(2)
    val ds = env.socketTextStream("x.x.x.x",9999)

    AsyncDataStream
      .unorderedWait(ds,new AsyncRequest,8000, TimeUnit.MICROSECONDS,100)
      .print("out put: ")
    env.execute("async")
  }

}


class AsyncRequest extends AsyncFunction[String,String] {
  var i = 0

  implicit lazy val executor: ExecutionContext = ExecutionContext.fromExecutor(Executors.directExecutor())
  override def asyncInvoke(input: String, resultFuture: ResultFuture[String]): Unit = {
    val f = Future {
      i+=1
      val str = "result_"+i
      println(s"in async recv $str")
      Thread sleep 1
      str
    }

    f onComplete {
      case Success(value) =>
        println(s"in async send ${value}")
        resultFuture.complete(Collections.singletonList(value))
      case Failure(exception) => exception.printStackTrace()

    }
  }
}
