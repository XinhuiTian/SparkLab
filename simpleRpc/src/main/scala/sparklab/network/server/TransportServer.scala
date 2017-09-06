package sparklab.network.server

import java.io.Closeable
import java.net.InetSocketAddress

import io.netty.bootstrap.ServerBootstrap
import io.netty.buffer.{ByteBufAllocator, PooledByteBufAllocator}
import io.netty.channel.{Channel, ChannelFuture, ChannelInitializer, ChannelOption}
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.SocketChannel
import io.netty.channel.socket.nio.NioServerSocketChannel
import io.netty.util.concurrent.DefaultThreadFactory
// import org.slf4j.LoggerFactory
import sparklab.network.TransportContext
import sparklab.network.client.{TransportClient, TransportResponseHandler}
import sparklab.network.protocol.ResponseMessage

/**
 * Created by XinhuiTian on 17/6/9.
 */
class TransportServer(
    context: TransportContext,
    hostToBind: String,
    portToBind: Int,
    appRpcHandler: RpcHandler
) extends Closeable {

  // private val logger = LoggerFactory.getLogger (classOf [TransportServer])
  var bootstrap: ServerBootstrap = _
  var channelFuture: ChannelFuture = _
  var port: Int = -1

  def getPort(): Int = port

  // start the server
  try {
    init (hostToBind, portToBind)
  } catch {
    case e: RuntimeException => this.close (); println("Cannot start server")
      throw e
  }

  def init(hostToString: String, portToBind: Int): Unit = {
    val bossGroup = new NioEventLoopGroup (0, new DefaultThreadFactory ("RPC", true))
    val workerGroup = bossGroup
    val allocator = new PooledByteBufAllocator (0, 0, 8192, 11)

    // create a bootstrap and config it
    bootstrap = new ServerBootstrap ()
      .group (bossGroup, workerGroup)
      .channel (classOf [NioServerSocketChannel])
      .option [ByteBufAllocator](ChannelOption.ALLOCATOR, allocator)
      .childOption [ByteBufAllocator](ChannelOption.ALLOCATOR, allocator)

    //specifies ChannelInitalizer that will be called for each accepted connection
    bootstrap.childHandler (new ChannelInitializer[SocketChannel] {
      override def initChannel(ch: SocketChannel): Unit = {
        // var rpcHandler = appRpcHandler
        initializePipeline (ch, appRpcHandler)
      }
    })

    val address =
      if (hostToString == null)
        new InetSocketAddress (portToBind)
      else
        new InetSocketAddress (hostToBind, portToBind)

    channelFuture = bootstrap.bind (address)
    channelFuture.syncUninterruptibly ()

    port = channelFuture.channel ().localAddress ().asInstanceOf [InetSocketAddress].getPort ()
    println("Shuffle server started on port: {}", port)

    // channelFuture.channel().closeFuture().sync()

  }

  // add the channel handler top the channel pipeline
  def initializePipeline(channel: SocketChannel, channelRpcHandler: RpcHandler)
  : TransportChannelHandler = {
    try {
      context.initializePipeline(channel, channelRpcHandler)
    } catch {
      case e: RuntimeException => println (e.getMessage); null
    }
  }




  def close(): Unit = {
    if (channelFuture != null) {
      channelFuture.channel ().close ().awaitUninterruptibly (10)
      channelFuture = null
    }

    if (bootstrap != null && bootstrap.group () != null) {
      bootstrap.group ().shutdownGracefully ()
    }

    if (bootstrap != null && bootstrap.childGroup () != null) {
      bootstrap.childGroup ().shutdownGracefully ()
    }
    bootstrap = null
  }
}
