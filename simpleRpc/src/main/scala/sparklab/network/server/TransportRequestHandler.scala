package sparklab.network.server

import java.nio.ByteBuffer

import com.google.common.base.Throwables
import io.netty.channel.{Channel, ChannelFuture, ChannelFutureListener}
import sparklab.network.buffer.NioManagedBuffer
import sparklab.network.client.{RpcResponseCallback, TransportClient}
import sparklab.network.protocol._

class TransportRequestHandler(
    channel: Channel,
    reverseClient: TransportClient,
    rpcHandler: RpcHandler) extends MessageHandler[RequestMessage] {

  def channelActive() = ???

  def exceptionCaught(cause: Throwable) = ???

  def channelInactive() = ???

  def handle(message: RequestMessage): Unit = {
    if (message.isInstanceOf[OneWayMessage]) {
      println("Handle OneWayMessage")
      processOneWayMessage(message.asInstanceOf[OneWayMessage])
    } else if (message.isInstanceOf [RpcRequest]) {
      processRpcRequest (message.asInstanceOf[RpcRequest])
    } else {
      throw new IllegalArgumentException("Unknow here")
    }
  }

  private def processRpcRequest(req: RpcRequest) = {
    try
      rpcHandler.receive (reverseClient, req.body.nioByteBuffer, new RpcResponseCallback () {
        override def onSuccess(response: ByteBuffer): Unit = {
          respond (new RpcResponse (req.requestId, new NioManagedBuffer (response)))
        }

        override

        def onFailure(e: Throwable): Unit = {
          respond (new RpcFailure (req.requestId, Throwables.getStackTraceAsString (e)))
        }
      })
    catch {
      case e: Exception =>
        // logger.error ("Error while invoking RpcHandler#receive() on RPC id " + req.requestId, e)
        respond (new RpcFailure (req.requestId, Throwables.getStackTraceAsString (e)))
    } finally req.body.release
  }

  private def processOneWayMessage(message: OneWayMessage) = {
    try {
      rpcHandler.receive(reverseClient, message.body.nioByteBuffer)
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      message.body.release
    }
  }

  private def respond(result: Encodable) = {
    val remoteAddress = channel.remoteAddress
    channel.writeAndFlush (result).addListener (new ChannelFutureListener () {
      @throws[Exception]
      override def operationComplete(future: ChannelFuture): Unit = {
        if (future.isSuccess) println (s"Sent result $result to client $remoteAddress")
        else {
          println(String.format ("Error sending result %s to %s; closing connection", result, remoteAddress), future.cause)
          channel.close
        }
      }
    })
  }
}