package sparklab.network.server

import io.netty.buffer.{ByteBuf, Unpooled}
import io.netty.channel.{ChannelHandlerContext, SimpleChannelInboundHandler}
import io.netty.util.CharsetUtil
import sparklab.network.client.{TransportClient, TransportResponseHandler}
import sparklab.network.protocol.{Message, OneWayMessage, RequestMessage, ResponseMessage}

class TransportChannelHandler(
    val client: TransportClient,
    val responseHandler: TransportResponseHandler,
    requestHandler: TransportRequestHandler,
    requestTimeoutMs: Long
) extends SimpleChannelInboundHandler[Message] {
  override def channelActive(ctx: ChannelHandlerContext): Unit = {
    println("Active channel!")
    // ctx.channel().writeAndFlush (Unpooled.copiedBuffer ("Netty rocks!", CharsetUtil.UTF_8))

  }

  /*
  override def channelRead(ctx: ChannelHandlerContext, msg: Any): Unit = {
    val in = msg.asInstanceOf [ByteBuf]
    System.out.println ("Server received: " + in.toString (CharsetUtil.UTF_8))
    ctx.write (in)
    println("Get Message!")
    println("Get Message!")
  }
  */
  override def channelRead0(ctx: ChannelHandlerContext, request: Message): Unit = {
    println("Get Message!" + request.toString)

    if (request.isInstanceOf [RequestMessage]) {
      println("RequestMessage: " + request.getClass.getSimpleName)
      println("request body is null? " + (request.body() == null))
      requestHandler.handle (request.asInstanceOf [RequestMessage])
    } else {
      println("Response here!")
      responseHandler.handle (request.asInstanceOf [ResponseMessage])
    }
  }
}
