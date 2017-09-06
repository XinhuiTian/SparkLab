package sparklab.network.client

import java.util.concurrent.ConcurrentHashMap

import io.netty.channel.Channel
import sparklab.network.protocol.{ResponseMessage, RpcResponse}

class TransportResponseHandler(channel: Channel) extends MessageHandler[ResponseMessage] {

  val outstandingRpcs = new ConcurrentHashMap[Long, RpcResponseCallback]()

  def addRpcRequest(requestId: Long, callback: RpcResponseCallback): Unit = {
    outstandingRpcs.put(requestId, callback)
  }

  override def handle(message: ResponseMessage): Unit = {
    if (message.isInstanceOf[RpcResponse]) {
      val resp = message.asInstanceOf[RpcResponse]
      val listener = outstandingRpcs.get(resp.requestId)
      if (listener == null) {
        println("Ignoring response")
      } else {
        outstandingRpcs.remove(resp.requestId)
        try {
          listener.onSuccess(resp.body().nioByteBuffer())
        } finally {
          resp.body().release()
        }
      }
    } else {
      println("no message")
    }
  }

  override def channelActive(): Unit = ???

  override def exceptionCaught(cause: Throwable): Unit = ???

  override def channelInactive(): Unit = ???


}
