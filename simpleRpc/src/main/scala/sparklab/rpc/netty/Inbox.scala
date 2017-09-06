package sparklab.rpc.netty

import javax.annotation.concurrent.GuardedBy

import sparklab.rpc.{RpcAddress, RpcEndpoint, ThreadSafeRpcEndpoint}

import scala.util.control.NonFatal

private[netty] sealed trait InboxMessage

case class OneWayInboxMessage(
    senderAddress: RpcAddress,
    content: Any) extends InboxMessage

case class RpcInboxMessage(
    senderAddress: RpcAddress,
    content: Any,
    context: NettyRpcCallContext) extends InboxMessage

private[netty] case object OnStart extends InboxMessage

private[netty] case object OnStop extends InboxMessage

/** A message to tell all endpoints that a remote process has connected. */
private[netty] case class RemoteProcessConnected(remoteAddress: RpcAddress) extends InboxMessage

/** A message to tell all endpoints that a remote process has disconnected. */
private[netty] case class RemoteProcessDisconnected(remoteAddress: RpcAddress) extends InboxMessage

/** A message to tell all endpoints that a sparklab.network error has happened. */
private[netty] case class RemoteProcessConnectionError(cause: Throwable, remoteAddress: RpcAddress)
  extends InboxMessage

// An inbox that stores messages for an RpcEndpoint and posts messages to it thread-safely
private[netty] class Inbox(
    val endpointRef: NettyRpcEndpointRef,
    val endpoint: RpcEndpoint) {

  inbox =>

  @GuardedBy ("this")
  protected val messages = new java.util.LinkedList[InboxMessage]()

  /** True if the inbox (and its associated endpoint) is stopped. */
  @GuardedBy ("this")
  private var stopped = false

  /** Allow multiple threads to process messages at the same time. */
  @GuardedBy ("this")
  private var enableConcurrent = false

  /** The number of threads processing messages for this inbox. */
  @GuardedBy ("this")
  private var numActiveThreads = 0

  // OnStart should be the first message to process
  inbox.synchronized{
    messages.add (OnStart)
  }

  def post(message: InboxMessage): Unit = inbox.synchronized {
    if (stopped) {
      onDrop(message)
    } else {
      messages.add(message)
      false
    }
  }

   protected def onDrop(message: InboxMessage): Unit = {
    // logWarning(s"Drop $message because $endpointRef is stopped")
  }

  def stop(): Unit = inbox.synchronized {
    if (!stopped) {
      enableConcurrent = false
      stopped = true
      messages.add (OnStop)
    }
  }

  def process(dispatcher: Dispatcher): Unit = {
    var message: InboxMessage = null
    inbox.synchronized{
      if (!enableConcurrent && numActiveThreads != 0) {
        return
      }

      message = messages.poll ()
      if (message != null) {
        numActiveThreads += 1
      } else {
        return
      }
    }

    while (true) {
      safelyCall (endpoint){
        message match {
          case RpcInboxMessage (_sender, content, context) =>
            try {
              println("Get RpcMessage")
              endpoint.receiveAndReply (context).applyOrElse [Any, Unit](content, {msg =>
                throw new Exception (s"Unsupported mesage $message from ${_sender}")
              })
            } catch {
              case NonFatal (e) =>
                context.sendFailure (e)
                // Throw the exception -- this exception will be caught by the safelyCall function.
                // The endpoint's onError function will be called.
                throw e
            }

          case OneWayInboxMessage(_sender, content) =>
            println("Get one message from " + _sender + " content: " + content.toString)

          case OnStart =>
            endpoint.onStart ()
            println("Get the onStart message")
            if (!endpoint.isInstanceOf [ThreadSafeRpcEndpoint]) {
              inbox.synchronized{
                if (!stopped) {
                  enableConcurrent = true
                }
              }
            }
        }
      }

      inbox.synchronized{
        // "enableConcurrent" will be set to false after `onStop` is called, so we should check it
        // every time.
        if (!enableConcurrent && numActiveThreads != 1) {
          // If we are not the only one worker, exit
          numActiveThreads -= 1
          return
        }
        message = messages.poll ()
        if (message == null) {
          numActiveThreads -= 1
          return
        }
      }
    }
  }

  // catch the error
  private def safelyCall(endpoint: RpcEndpoint)(action: => Unit): Unit = {
    try action catch {
      case NonFatal (e) =>
        try endpoint.onError (e) catch {
          case NonFatal (ee) => println ("Ignoring error " + ee)
        }
    }
  }
}
