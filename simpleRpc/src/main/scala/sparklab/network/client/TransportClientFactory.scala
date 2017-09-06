package sparklab.network.client

import java.io.IOException
import java.net.{InetSocketAddress, SocketAddress}
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicReference

import io.netty.bootstrap.Bootstrap
import io.netty.buffer.{ByteBufAllocator, PooledByteBufAllocator}
import io.netty.channel.{Channel, ChannelInitializer, ChannelOption}
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.SocketChannel
import io.netty.channel.socket.nio.NioSocketChannel
import io.netty.util.concurrent.DefaultThreadFactory
import sparklab.network.TransportContext
import sparklab.network.server.TransportChannelHandler

import scala.util.Random

class ClientPool(size: Int) {
  val clients: Array[TransportClient] = new Array[TransportClient](size)
  val locks: Array[AnyVal] = new Array[AnyVal](size)
  // for (i <- 0 until size)
    // locks(i) = new AnyVal()
}

class TransportClientFactory(context: TransportContext) {
  val socketChannel = new NioSocketChannel()
  val workerGroup = new NioEventLoopGroup(0, new DefaultThreadFactory ("RPC", true))
  val pooledAllocater = new PooledByteBufAllocator (0, 0, 8192, 11)
  // private var connectionPool = new ConcurrentHashMap[SocketAddress, ClientPool]()

  def rand: Random = new Random()

  def createClient(remoteHost: String, remotePort: Int): TransportClient = {
    /*
    val unresolvedAddress = InetSocketAddress.createUnresolved(remoteHost, remotePort)
    var clientPool = connectionPool.get(unresolvedAddress)
    if (clientPool == null) {
      connectionPool.putIfAbsent(unresolvedAddress, new ClientPool(2))
      clientPool = connectionPool.get(unresolvedAddress)
    }

    val clientIndex = rand.nextInt(2)
    var cachedClient = clientPool.clients(clientIndex)
    */
    val resolvedAddress = new InetSocketAddress (remoteHost, remotePort)
    val client = createClient(resolvedAddress)
    client

    /*

    if (cachedClient != null) { // Make sure that the channel will not timeout by updating
      // the last use time of the
      // handler. Then check that the client is still alive, in case it timed out before
      // this code was able to update things.
      val handler = cachedClient.getChannel.pipeline.get (classOf [TransportChannelHandler])
      handler.synchronized {
        handler.responseHandler
      }

      if (cachedClient.isActive) {
        // logger.trace ("Returning cached connection to {}: {}", cachedClient.getSocketAddress, cachedClient)
        return cachedClient
      }
    }

    // clientPool.locks (clientIndex).synchronized {
      cachedClient = clientPool.clients (clientIndex)

      if (cachedClient != null) {
        if (cachedClient.isActive) {
          // logger.trace ("Returning cached connection to {}: {}", resolvedAddress, cachedClient)
          return cachedClient
        } //logger.info ("Found inactive connection to {}, creating a new one.", resolvedAddress)
      }

      clientPool.clients.update(clientIndex, createClient (resolvedAddress))

      return clientPool.clients (clientIndex)
    // }
    */
  }

  def createClient(address: InetSocketAddress): TransportClient = {
    val bootstrap = new Bootstrap()
    bootstrap.group(workerGroup)
      .channel(classOf[NioSocketChannel])
      .option[java.lang.Boolean](ChannelOption.TCP_NODELAY, true)
      .option[java.lang.Boolean](ChannelOption.SO_KEEPALIVE, true)
      .option[java.lang.Integer](ChannelOption.CONNECT_TIMEOUT_MILLIS, 120000)
      .option[ByteBufAllocator](ChannelOption.ALLOCATOR, pooledAllocater)

    val clientRef = new AtomicReference[TransportClient]()
    val channelRef = new AtomicReference[Channel]()

    bootstrap.handler(new ChannelInitializer[SocketChannel] {
      override def initChannel(ch: SocketChannel) = {
        val clientHandler = context.initializePipeline(ch)
        clientRef.set(clientHandler.client)
        channelRef.set(ch)
      }
    })


    val cf = bootstrap.connect(address)

    if (!cf.awaitUninterruptibly(120000)) {
      // throw new IOException(String.format("Connecting to %s timed out (%s ms)", address, 120000))
    }
    if (cf.cause != null) {
      // throw new IOException (String.format ("Failed to connect to %s", address), cf.cause)
    }



    val client = new TransportClient(cf.channel(), clientRef.get().handler)

    println("Successfully created connection to " + address.getHostString)
    client
  }
}
