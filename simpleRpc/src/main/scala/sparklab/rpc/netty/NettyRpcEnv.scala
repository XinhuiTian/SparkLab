package sparklab.rpc.netty

import java.io.{ObjectInputStream, ObjectOutputStream, Serializable}
import java.net.InetSocketAddress
import java.nio.ByteBuffer
import java.util.concurrent.{ConcurrentHashMap, TimeoutException}
import java.util.concurrent.atomic.AtomicBoolean

import com.sun.istack.internal.Nullable
import io.netty.buffer.Unpooled
import sparklab.network.TransportContext
import sparklab.network.client.{RpcResponseCallback, TransportClient}
import sparklab.network.server.{RpcHandler, TransportServer, TransportServerBootstrap}
import sparklab.rpc._
import sparklab.serialize.{JavaSerializer, JavaSerializerInstance, SerializerInstance}
import sparklab.util.{NetworkUtils, ThreadUtils}
import sun.rmi.server.Dispatcher

import scala.concurrent.{Future, Promise}
import scala.reflect.ClassTag
import scala.util.{DynamicVariable, Failure, Success}
import scala.util.control.NonFatal

private[netty] case class RpcFailure(e: Throwable)

class NettyRpcEnv(
    javaSerializerInstance: SerializerInstance,
    host: String) extends RpcEnv {
  @Nullable
  override lazy val address: RpcAddress = {
    if (server != null) RpcAddress (host, server.getPort ()) else null
  }
  private val stopped = new AtomicBoolean (false)

  private val dispatcher: Dispatcher = new Dispatcher (this)

  /**
   * A map for [[RpcAddress]] and [[Outbox]]. When we are connecting to a remote [[RpcAddress]],
   * we just put messages to its [[Outbox]] to implement a non-blocking `send` method.
   */
  private val outboxes = new ConcurrentHashMap[RpcAddress, Outbox]()

  private val transportContext = new TransportContext (new NettyRpcHandler (dispatcher, this))

  private[netty] val clientConnectionExecutor = ThreadUtils.newDaemonCachedThreadPool(
    "netty-rpc-connection", 64)
  @volatile private var server: TransportServer = _

  private val clientFactory = transportContext.createClientFactory()

  def startServer(bindAddress: String, port: Int): Unit = {
    // val bootstraps = List.empty [TransportServerBootstrap]

    server = transportContext.createServer (bindAddress, port)
    // a default rpcendpoint to prove its existence
    dispatcher.registerRpcEndpoint (RpcEndpointVerifier.NAME,
      new RpcEndpointVerifier (this, dispatcher))
  }

  override def setupEndpoint(name: String, endpoint: RpcEndpoint): RpcEndpointRef = {
    dispatcher.registerRpcEndpoint (name, endpoint)
  }

  override def asyncSetupEndpointRefByURI(uri: String): Future[RpcEndpointRef] = {
    val addr = RpcEndpointAddress (uri)
    println ("setup endpoint: " + addr)
    val endpointRef = new NettyRpcEndpointRef (addr, this) // create ref
    // val verifier = new NettyRpcEndpointRef(
    //   RpcEndpointAddress(addr.rpcAddress, RpcEndpointVerifier.NAME), this)

    // currently do not verify
    Future.successful (endpointRef)
  }

  private[netty] def createClient(address: RpcAddress): TransportClient = {
    clientFactory.createClient(address.host, address.port)
  }

  def ask[T: ClassTag](message: RequestMessage, timeout: Long): Future[T] = {
    val promise = Promise [Any]()
    val remoteAddr = message.receiver.address

    def onFailure(e: Throwable): Unit = {
      if (!promise.tryFailure (e)) {
        // logWarning(s"Ignored failure: $e")
      }
    }

    def onSuccess(reply: Any): Unit = reply match {
      case RpcFailure (e) => onFailure (e)
      case rpcReply =>
        if (!promise.trySuccess (rpcReply)) {
          // logWarning(s"Ignored message: $reply")
        }
    }

    try {
      println (remoteAddr)
      if (remoteAddr == address) {
        println ("local post")
        val p = Promise [Any]()
        p.future.onComplete{
          case Success (response) => onSuccess (response)
          case Failure (e) => onFailure (e)
        }(ThreadUtils.sameThread)
        dispatcher.postLocalMessage (message, p)
      } else {
        val rpcMessage = RpcOutboxMessage (serialize(message), onFailure,
          (client, response) => onSuccess(deserialize[Any](client, response)))
        // val rpcMessage = OneWayOutboxMessage (Unpooled.copyInt(1).nioBuffer())
          // onFailure,
          // (client, response) => onSuccess (deserialize [Any](client, response)))
        postToOutbox (message.receiver, rpcMessage)
        promise.future.onFailure{
          // case _: TimeoutException => rpcMessage.onTimeout ()
          case _ =>
        }(ThreadUtils.sameThread)
      }

    } catch {
      case NonFatal (e) =>
        onFailure (e)
    }

    promise.future.mapTo [T].recover (addMessageIfTimeout)(ThreadUtils.sameThread)
  }

  def addMessageIfTimeout[T]: PartialFunction[Throwable, T] = {
    // The exception has already been converted to a RpcTimeoutException so just raise it
    case rte: RpcTimeoutException => throw rte
    // Any other TimeoutException get converted to a RpcTimeoutException with modified message
    case te: TimeoutException => throw new RpcTimeoutException ("Timeout", te)
  }

  private[netty] def serialize(content: Any): ByteBuffer = {
    javaSerializerInstance.serialize (content)
  }

  private[netty] def deserialize[T: ClassTag](client: TransportClient, bytes: ByteBuffer): T = {

    /*NettyRpcEnv.currentClient.withValue (client) {
      deserialize{() =>
        javaSerializerInstance.deserialize [T](bytes)
      }
    }
    */
    javaSerializerInstance.deserialize[T](bytes)
  }

  def deserialize[T](deserializationAction: () => T): T = {
    NettyRpcEnv.currentEnv.withValue (this){
      deserializationAction ()
    }
  }

private def postToOutbox(receiver: NettyRpcEndpointRef, message: OutboxMessage): Unit = {
    // 1. find the outbox
    if (receiver.client != null) {
      message.sendWith(receiver.client)
    } else {
      val targetOutbox = {
        val outbox = outboxes.get(receiver.address)
        if (outbox == null) {
          val newOutbox = new Outbox(this, receiver.address)
          val oldOutbox = outboxes.putIfAbsent(receiver.address, newOutbox)
          if (oldOutbox == null) {
            newOutbox
          } else {
            oldOutbox
          }
        } else {
          outbox
        }
      }
      if (stopped.get) {
        // It's possible that we put `targetOutbox` after stopping. So we need to clean it.
        outboxes.remove(receiver.address)
        targetOutbox.stop()
      } else {
        println("outbox send message")
        targetOutbox.send(message)
      }
    }
  }


  override def endpointRef(endpoint: RpcEndpoint): RpcEndpointRef = {
    dispatcher.getRpcEndpointRef (endpoint)
  }

  override def stop(endpointRef: RpcEndpointRef): Unit = {
    require (endpointRef.isInstanceOf [NettyRpcEndpointRef])
    dispatcher.stop (endpointRef)
  }

  override def shutdown() = {
    cleanup ()
  }

  /*
  override def awaitTermination(): Unit = {
    // dispatcher.awaitTermination()
  }
  */

  private def cleanup(): Unit = {
    if (!stopped.compareAndSet (false, true)) {
      return
    }

    // stop all the outbox
    val iter = outboxes.values ().iterator ()
    while (iter.hasNext) {
      val outbox = iter.next ()
      outboxes.remove (outbox.address)
      outbox.stop ()
    }
  }

  override def awaitTermination(): Unit = {
    dispatcher.awaitTermination()
  }
}

private[netty] object NettyRpcEnv {

  /**
   * When deserializing the [[NettyRpcEndpointRef]], it needs a reference to [[NettyRpcEnv]].
   * Use `currentEnv` to wrap the deserialization codes. E.g.,
   *
   * {{{
   *   NettyRpcEnv.currentEnv.withValue(this) {
   *     your deserialization codes
   *   }
   * }}}
   */
  private[netty] val currentEnv = new DynamicVariable[NettyRpcEnv](null)

  /**
   * Similar to `currentEnv`, this variable references the client instance associated with an
   * RPC, in case it's needed to find out the remote address during deserialization.
   */
  private[netty] val currentClient = new DynamicVariable[TransportClient](null)

}


class NettyRpcEnvFactory extends RpcEnvFactory {

  // create a RpcEnv
  def create(config: RpcEnvConfig): RpcEnv = {
    // create a serializer instance for message serialization
    val javaSerializerInstance = new JavaSerializer().newInstance()
    // create a rpc env
    val rpcEnv = new NettyRpcEnv (javaSerializerInstance, config.advertiseAddress)

    if (!config.clientMode) {
      val startNettyRpcEnv: Int => (NettyRpcEnv, Int) = {actualPort =>
        rpcEnv.startServer (config.bindAddress, actualPort)
        (rpcEnv, rpcEnv.address.port)
      }
      try {
        NetworkUtils.startServiceOnPort (config.port, startNettyRpcEnv, config.name)._1
      } catch {
        case NonFatal (e) =>
          rpcEnv.shutdown ()
          throw e
      }
    }
    rpcEnv
  }
}

class NettyRpcEndpointRef(
    endpointAddress: RpcEndpointAddress,
    @transient @volatile private var nettyEnv: NettyRpcEnv)
  extends RpcEndpointRef with Serializable {

  private val _address = if (endpointAddress.rpcAddress != null) endpointAddress else null
  private val _name = endpointAddress.name
  @transient
  @volatile var client: TransportClient = _

  def address: RpcAddress = if (_address != null) _address.rpcAddress else null

  override def name: String = if (_name != null) _name else null

  private def readObject(in: ObjectInputStream): Unit = {
    in.defaultReadObject()
    nettyEnv = NettyRpcEnv.currentEnv.value
    client = NettyRpcEnv.currentClient.value
  }

  private def writeObject(out: ObjectOutputStream): Unit = {
    out.defaultWriteObject()
  }

  def ask[T: ClassTag](message: Any, timeout: Long): Future[T] = {
    nettyEnv.ask (RequestMessage (nettyEnv.address, this, message), timeout)
  }

  final override def equals(that: Any): Boolean = that match {
    case other: NettyRpcEndpointRef => _address == other._address
    case _ => false
  }

  final override def hashCode(): Int = if (_address == null) 0 else _address.hashCode()
}

case class RequestMessage(senderAddress: RpcAddress, receiver: NettyRpcEndpointRef, content: Any)

class NettyRpcHandler(dispatcher: Dispatcher, nettyEnv: NettyRpcEnv) extends RpcHandler {
  override def receive(
      client: TransportClient,
      message: ByteBuffer,
      callback: RpcResponseCallback): Unit = {
    val messageToDispatch = internalReceive(client, message)
    dispatcher.postRemoteMessage(messageToDispatch, callback)
  }

  override def receive(
      client: TransportClient,
      message: ByteBuffer): Unit = {
    val messageToDispatch = internalReceive(client, message)
    dispatcher.postOneWayMessage(messageToDispatch)
  }


  private def internalReceive(client: TransportClient, message: ByteBuffer): RequestMessage = {
    // get the resource addr
    val addr = client.getChannel().remoteAddress().asInstanceOf[InetSocketAddress]
    println("Remote addr: " + addr)

    val clientAddr = RpcAddress(addr.getHostString, addr.getPort)
    val requestMessage = nettyEnv.deserialize[RequestMessage](client, message)
      // message.getInt()
    println("Message: " + requestMessage)
    RequestMessage(clientAddr, null, message)

    if (requestMessage.senderAddress == null) {
      RequestMessage(clientAddr, requestMessage.receiver, requestMessage.content)
    } else {
      requestMessage
    }
  }
}