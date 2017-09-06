package sparklab.rpc.netty

import java.nio.ByteBuffer
import java.nio.charset.Charset
import java.util.concurrent.Callable
import javax.annotation.concurrent.GuardedBy

import io.netty.buffer.Unpooled
import io.netty.util.CharsetUtil
import sparklab.network.client.{RpcResponseCallback, TransportClient}
import sparklab.rpc.RpcAddress

import scala.util.control.NonFatal

sealed trait OutboxMessage {

  def sendWith(client: TransportClient): Unit

  def onFailure(e: Throwable): Unit
}

case class OneWayOutboxMessage(val content: ByteBuffer) extends OutboxMessage {

  override def sendWith(client: TransportClient): Unit = {
    client.send(content)
  }

  override def onFailure(e: Throwable): Unit = {
    e match {
      case e1: Throwable => println (s"Failed to send one-way RPC." + e1)
    }
  }
}

case class RpcOutboxMessage(
    val content: ByteBuffer,
    _onFailure: (Throwable) => Unit,
    _onSuccess: (TransportClient, ByteBuffer) => Unit)
  extends OutboxMessage with RpcResponseCallback {

  private var client: TransportClient = _
  private var requestId: Long = _

  override def sendWith(client: TransportClient): Unit = {
    this.client = client
    println("Send with client")
    this.requestId = client.sendRpc(content, this)
    // this.requestId = client.sendRpc(content, this)
  }

  def onTimeout(): Unit = {
    require (client != null, "TransportClient has not yet been set.")
    // client.removeRpcRequest(requestId)
  }

  override def onFailure(e: Throwable): Unit = {
    _onFailure (e)
  }

  override def onSuccess(response: ByteBuffer): Unit = {
    _onSuccess (client, response)
  }
}

// each outbox have one related client
class Outbox(nettyEnv: NettyRpcEnv, val address: RpcAddress) {
  outbox =>

  @GuardedBy ("this")
  private val messages = new java.util.LinkedList[OutboxMessage]

  @GuardedBy ("this")
  private var client: TransportClient = null

  @GuardedBy ("this")
  private var connectFuture: java.util.concurrent.Future[Unit] = null

  @GuardedBy ("this")
  private var stopped = false

  /**
   * If there is any thread draining the message queue
   */
  @GuardedBy ("this")
  private var draining = false

  def send(message: OutboxMessage): Unit = {
    val dropped = synchronized{
      if (stopped) {
        true
      } else {
        messages.add (message)
        false
      }
    }

    if (dropped) {
      message.onFailure (new Exception ("Message is dropped because Outbox is stopped"))
    } else {
      drainOutbox ()
    }
  }

  private def closeClient(): Unit = synchronized {
    // Just set client to null. Don't close it in order to reuse the connection.
    client = null
  }

  private def drainOutbox(): Unit = {
    println("DrainOutbox")
    var message: OutboxMessage = null
    synchronized{
      if (stopped) {
        return
      }
      if (connectFuture != null) {
        return
      }
      if (client == null) { // whether have the client
        println("no client")
        launchConnectTask()
        return
      }
      if (draining) { // whether need to drain the message queue
        return
      }
      message = messages.poll ()
      if (message == null) { // no message then return
        println("No message")
        return
      }
      draining = true
    }

    while (true) {
      try {
        val _client = synchronized { client }
        println("client == null? " + (_client == null))
        if (_client != null) {
          println("Sent to server")
          message.sendWith(_client)
        } else {
          assert(stopped == true)
        }
      } catch {
        case NonFatal(e) =>
          return
      }

      synchronized {
        if (stopped) {
          return
        }
        message = messages.poll()
        if (message == null) {
          draining = false

          return
        }
      }
    }
  }

  private def launchConnectTask(): Unit = {
    // connectFuture = nettyEnv.clientConnectionExecutor.submit(new Callable[Unit] {

    //   override def call(): Unit = {
        try {
          val _client = nettyEnv.createClient(address)
          println("Create client: " + address.host + ":" + address.port)
          outbox.synchronized {
            client = _client
            if (stopped) {
              println("Close client")
              closeClient()
            }
          }
        } catch {
          case ie: InterruptedException =>
            // exit
            return
          case NonFatal(e) =>
            outbox.synchronized { connectFuture = null }
            // handleNetworkFailure(e)
            return
        }
        outbox.synchronized { connectFuture = null }
        // It's possible that no thread is draining now. If we don't drain here, we cannot send the
        // messages until the next message arrives.
        drainOutbox()
      // }
    // })
  }

  def stop(): Unit = {}
}

