package sparklab.network.client

import java.io.Closeable
import java.net.InetSocketAddress
import java.nio.ByteBuffer
import java.nio.charset.Charset
import java.util.UUID

import io.netty.buffer.Unpooled
import io.netty.channel.{Channel, ChannelFuture, ChannelFutureListener}
import io.netty.util.CharsetUtil
import sparklab.network.buffer.NioManagedBuffer
import sparklab.network.protocol.{OneWayMessage, RpcRequest}

class TransportClient(
    channel: Channel,
    val handler: TransportResponseHandler) extends Closeable {

  def getChannel() = channel

  def getSocketAddress() = channel.remoteAddress ()

  def send(message: ByteBuffer) = {
    // println("TransportClient Send: " + Charset.forName("UTF-8").decode(message))
    // println(channel.isOpen + " " + channel.isActive)
    println(channel.localAddress().asInstanceOf[InetSocketAddress].toString)
    println(channel.remoteAddress().asInstanceOf[InetSocketAddress].toString)
    channel.writeAndFlush(new OneWayMessage(new NioManagedBuffer(message)))
      // channel.writeAndFlush(Unpooled.copiedBuffer ("Netty rocks!", CharsetUtil.UTF_8))
      .addListener (new ChannelFutureListener () {
      @throws[Exception]
      override def operationComplete(future: ChannelFuture): Unit = {
        if (future.isSuccess) {
          System.out.println ("Write Successfully")
        }
        else {
          System.out.println ("Write error")
          future.cause.printStackTrace ()
        }
      }
    })
    // cf.awaitUninterruptibly()
  }

  val requestId: Long = Math.abs (UUID.randomUUID.getLeastSignificantBits)

  def sendRpc(message: ByteBuffer, callback: RpcResponseCallback): Long = {
    handler.addRpcRequest(requestId, callback)
    channel.writeAndFlush(new RpcRequest(requestId, new NioManagedBuffer(message))).addListener(
      new ChannelFutureListener {
        override def operationComplete(future: ChannelFuture): Unit = {
          if (future.isSuccess) {
            println("Rpc Success")
          } else {
            channel.close()
            println("Rpc Error")
          }
        }
      }
    )
    requestId
  }

  def isActive: Boolean = {
    channel.isOpen || channel.isActive
  }

  override def close() = {
    channel.close ().awaitUninterruptibly (10)
  }
}
