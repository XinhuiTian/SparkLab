package sparklab.rpc

import scala.concurrent.Future
import scala.reflect.ClassTag

abstract class RpcEndpointRef extends Serializable {

  /**
   * return the address for the [[RpcEndpointRef]]
   */
  def address: RpcAddress

  def name: String

  /**
   * Sends a one-way asynchronous message. Fire-and-forget semantics.
   */
  // def send(message: Any): Unit

  /**
   * Send a message to the corresponding [[RpcEndpoint.receiveAndReply]]
   * and return a Future to receive the reply
   */
  def ask[T: ClassTag](message: Any, timeout: Long): Future[T]

  // fixed maxRetries and retryWaitMs
  def askWithRetry[T: ClassTag](message: Any, timeout: Long = 120): T = {
    var attempts = 0
    var lastException: Exception = null

    while (attempts < 5) {
      attempts += 1
      try {
        val future = ask [T](message, timeout)
        val rpcTimeout = RpcTimeout (timeout)
        val result = rpcTimeout.awaitResult (future)
        if (result == null) {
          throw new Exception ("RpcEndpoint returned null")
        }

        return result
      } catch {
        case ie: InterruptedException => throw ie
        case e: Exception =>
          lastException = e
          println (s"Error sending message [message = $message] in $attempts attempts", e)
      }

      if (attempts < 5) {
        Thread.sleep (3)
      }
    }

    throw new Exception (
      s"Error sending message [message = $message]", lastException)
  }
}
