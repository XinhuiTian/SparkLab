package sparklab.rpc.netty

import java.util.concurrent._
import javax.annotation.concurrent.GuardedBy

import sparklab.network.client.RpcResponseCallback
import sparklab.rpc.{RpcEndpoint, RpcEndpointAddress, RpcEndpointRef}
import sparklab.util.ThreadUtils

import scala.concurrent.Promise
import scala.util.control.NonFatal

// A message dispatcher, responsible for routing RPC messages to the appropriate endpoint(s).
class Dispatcher(nettyEnv: NettyRpcEnv) {

  private val endpoints: ConcurrentMap[String, EndpointData] =
    new ConcurrentHashMap[String, EndpointData]
  private val endpointRefs: ConcurrentMap[RpcEndpoint, RpcEndpointRef] =
    new ConcurrentHashMap[RpcEndpoint, RpcEndpointRef]
  // Track the receivers whose inboxes may contain messages.
  private val receivers = new LinkedBlockingQueue[EndpointData]
  private val threadpool: ThreadPoolExecutor = {
    val numThreads = 2
    val pool = ThreadUtils.newDaemonFixedThreadPool (numThreads, "dispatcher-event-loop")
    for (i <- 0 until numThreads) {
      pool.execute (new MessageLoop)
    }
    pool
  }
  @GuardedBy ("this")
  private var stopped = false

  def postToAll(message: InboxMessage) = {
    val iter = endpoints.keySet().iterator()
    while (iter.hasNext) {
      val name = iter.next
      postMessage(name, message, (e) => println(s"Message $message dropped. ${e.getMessage}"))
    }
  }

  def postRemoteMessage(message: RequestMessage, callback: RpcResponseCallback) = {
    val rpcCallContext =
      new RemoteNettyRpcCallContext(nettyEnv, callback, message.senderAddress)
    val rpcMessage = RpcInboxMessage(message.senderAddress, message.content, rpcCallContext)
    postMessage(message.receiver.name, rpcMessage, (e) => callback.onFailure(e))
  }

  def postLocalMessage(message: RequestMessage, p: Promise[Any]): Unit = {
    val rpcCallContext =
      new LocalNettyRpcCallContext(message.senderAddress, p)

    val rpcMessage = RpcInboxMessage(message.senderAddress, message.content, rpcCallContext)
    postMessage(message.receiver.name, rpcMessage, (e) => p.tryFailure(e))
  }

  def postOneWayMessage(message: RequestMessage): Unit = {
    postMessage(message.receiver.name,
      OneWayInboxMessage(message.senderAddress, message.content),
      (e) => throw e)
  }

  // get the endpoint, and post message to its inbox
  private def postMessage(
      endpointName: String,
      message: InboxMessage, callbackIfStopped: (Exception) => Unit): Unit = {
    val error = synchronized {
      val data = endpoints.get(endpointName)
      if (stopped) {
        Some(new Exception("Stopped"))
      } else if (data == null) {
        Some(new Exception(s"Could not find $endpointName"))
      } else {
        data.inbox.post(message)
        receivers.offer(data)
      }
    }
  }

  def stop(endpointRef: RpcEndpointRef): Unit = {
    synchronized{
      if (stopped) {
        return
      }
      unregisterRpcEndpoint (endpointRef.name)
    }
  }

  // Should be idempotent
  private def unregisterRpcEndpoint(name: String): Unit = {
    val data = endpoints.remove (name)
    if (data != null) {
      data.inbox.stop ()
      receivers.offer (data) // for the OnStop message
    }
    // Don't clean `endpointRefs` here because it's possible that some messages are being processed
    // now and they can use `getRpcEndpointRef`. So `endpointRefs` will be cleaned in Inbox via
    // `removeRpcEndpointRef`.
  }

  // put the name and RpcEndpoint into the hashmap
  def registerRpcEndpoint(name: String, endpoint: RpcEndpoint): NettyRpcEndpointRef = {
    val addr = RpcEndpointAddress (nettyEnv.address, name)
    val endpointRef = new NettyRpcEndpointRef (addr, nettyEnv)

    synchronized{
      if (endpoints.putIfAbsent (name, new EndpointData (name, endpoint, endpointRef)) != null) {
        throw new IllegalArgumentException (s"There is already an RpcEndpoint called $name")
      }
      val data = endpoints.get (name)
      endpointRefs.put (data.endpoint, data.ref)
      receivers.offer (data)
    }

    endpointRef
  }

  def getRpcEndpointRef(endpoint: RpcEndpoint): RpcEndpointRef = endpointRefs.get (endpoint)

  def awaitTermination(): Unit = {
    threadpool.awaitTermination(Long.MaxValue, TimeUnit.MILLISECONDS)
  }

  def verify(name: String): Boolean = {
    endpoints.containsKey (name)
  }

  private class EndpointData(
      val name: String,
      val endpoint: RpcEndpoint,
      val ref: NettyRpcEndpointRef) {
    val inbox = new Inbox (ref, endpoint)
  }

  private class MessageLoop extends Runnable {
    override def run(): Unit = {
      try {
        while (true) {
          try {
            val data = receivers.take ()
            data.inbox.process (Dispatcher.this)
          } catch {
            case NonFatal (e) => println (e)
          }
        }
      } catch {
        case ie: InterruptedException => // exit
      }
    }
  }
}
