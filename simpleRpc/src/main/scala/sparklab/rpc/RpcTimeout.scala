package sparklab.rpc

import java.util.concurrent.TimeoutException

import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import scala.util.control.NonFatal

class RpcTimeoutException(message: String, cause: TimeoutException)
  extends TimeoutException(message) { initCause(cause) }

class RpcTimeout(val duration: FiniteDuration) extends Serializable {
  def awaitResult[T](future: Future[T]): T = {
    val wrapAndRethrow: PartialFunction[Throwable, T] = {
      case NonFatal (t) =>
        throw new Exception ("Exception thrown in awaitResult", t)
    }
    try {
      Await.result (future, duration)
    } catch wrapAndRethrow
  }
}

object RpcTimeout {
  def apply(timeout: Long): RpcTimeout = {
    new RpcTimeout ({
      timeout.seconds
    })
  }
}
