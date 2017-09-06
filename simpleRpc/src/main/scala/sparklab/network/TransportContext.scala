package sparklab.network

import io.netty.channel.Channel
import io.netty.channel.socket.SocketChannel
import sparklab.network.client.{TransportClient, TransportClientFactory, TransportResponseHandler}
import sparklab.network.protocol.{MessageDecoder, MessageEncoder, ResponseMessage}
import sparklab.network.server._

/**
 * Created by XinhuiTian on 17/5/5.
 * Contains the context to create a TransportServer, TransportClientFactory,
 * and setup Netty Channel pipelines with a TransportChannelHandler
 */
class TransportContext(rpcHandler: RpcHandler) {
  // def createServer(bindAddress: String, port: Int, bootstraps: sparklab.util.List[Nothing]): _root_.sparkLike.sparklab.network
  // .server.TransportServer = ???

  val encoder = new MessageEncoder()
  val decoder = new MessageDecoder()
  /** Create a server which will attempt to bind to a specific host and port. */
  def createServer(host: String, port: Int)
  : TransportServer = new TransportServer (this, host, port, rpcHandler)

  /** Creates a new server, binding to any available ephemeral port. */
  def createServer(bootstraps: List[TransportServerBootstrap]): TransportServer = createServer (0, bootstraps)

  def createServer()
  : TransportServer = createServer(0, List [TransportServerBootstrap]())

  /** Create a server which will attempt to bind to a specific port. */
  def createServer(port: Int, bootstraps: List[TransportServerBootstrap])
  : TransportServer = new TransportServer (this, null, port, rpcHandler)

  def createClientFactory() = {
    new TransportClientFactory (this)
  }

  def createChannelHandler(channel: Channel, rpcHandler: RpcHandler): TransportChannelHandler = {
    val responseHandler = new TransportResponseHandler (channel)
    val client = new TransportClient (channel, responseHandler)
    val requestHandler = new TransportRequestHandler (channel, client, rpcHandler)
    new TransportChannelHandler (client, responseHandler, requestHandler, 120 * 1000)
  }

  def initializePipeline(channel: SocketChannel): TransportChannelHandler =
    initializePipeline(channel, rpcHandler)

  def initializePipeline(channel: SocketChannel, handler: RpcHandler): TransportChannelHandler = {
    val channelHandler = createChannelHandler(channel, handler)
    channel.pipeline()
       .addLast("encode", encoder)
       .addLast("decode", decoder)
      .addLast ("handler", channelHandler)
    channelHandler
  }
}