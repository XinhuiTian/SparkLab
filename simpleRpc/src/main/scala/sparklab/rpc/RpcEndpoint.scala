package sparklab.rpc

import sparklab.rpc.netty.Dispatcher

trait RpcEnvFactory {
  def create(config: RpcEnvConfig): RpcEnv
}

trait RpcEndpoint {

  /**
   * The [[RpcEnv]] that this [[RpcEndpoint]] is registered to.
   */
  val rpcEnv: RpcEnv

  /**
   * Process messages from [[RpcEndpointRef]] or [[RpcCallContext.reply)]]. If receiving a
   * unmatched message, [[Exception]] will be thrown and sent to `onError`.
   */
  def receive: PartialFunction[Any, Unit] = {
    case _ => throw new Exception (self + " does not implement 'receive'")
  }

  /**
   * Process messages from [[RpcEndpointRef.ask]]. If receiving a unmatched message,
   * [[Exception]] will be thrown and sent to `onError`.
   */
  def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case _ => context.sendFailure (new Exception (self + " won't reply anything"))
  }

  def onError(cause: Throwable): Unit = {
    throw cause
  }

  /**
   * Invoked when `remoteAddress` is connected to the current node.
   */
  def onConnected(remoteAddress: RpcAddress): Unit = {
    // By default, do nothing.
  }

  /**
   * Invoked when `remoteAddress` is lost.
   */
  def onDisconnected(remoteAddress: RpcAddress): Unit = {
    // By default, do nothing.
  }

  /**
   * Invoked when some sparklab.network error happens in the connection between the current node and
   * `remoteAddress`.
   */
  def onNetworkError(cause: Throwable, remoteAddress: RpcAddress): Unit = {
    // By default, do nothing.
  }

  /**
   * Invoked before [[RpcEndpoint]] starts to handle any message.
   */
  def onStart(): Unit = {
    // By default, do nothing.
  }

  /**
   * Invoked when [[RpcEndpoint]] is stopping. `self` will be `null` in this method and you cannot
   * use it to send or ask messages.
   */
  def onStop(): Unit = {
    // By default, do nothing.
  }

  /**
   * A convenient method to stop [[RpcEndpoint]].
   */
  final def stop(): Unit = {
    val _self = self
    if (_self != null) {
      rpcEnv.stop (_self)
    }
  }

  final def self: RpcEndpointRef = {
    require (rpcEnv != null, "rpcEnv has not been initialized")
    rpcEnv.endpointRef (this)
  }
}

trait ThreadSafeRpcEndpoint extends RpcEndpoint

trait RpcCallContext {

  /**
   * Reply a message to the sender. If the sender is [[RpcEndpoint]], its [[RpcEndpoint.receive]]
   * will be called.
   */
  def reply(response: Any): Unit

  /**
   * Report a failure to the sender.
   */
  def sendFailure(e: Throwable): Unit

  /**
   * The sender of this message.
   */
  def senderAddress: RpcAddress
}

class RpcEndpointVerifier(override val rpcEnv: RpcEnv, dispatcher: Dispatcher)
  extends RpcEndpoint {

  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case RpcEndpointVerifier.CheckExistence (name) => context.reply (dispatcher.verify (name))
  }
}

object RpcEndpointVerifier {
  val NAME = "endpoint-verifier"

  /** A message used to ask the remote [[RpcEndpointVerifier]] if an [[RpcEndpoint]] exists. */
  case class CheckExistence(name: String)

}
