package sparklab.rpc.netty

import sparklab.network.client.RpcResponseCallback
import sparklab.rpc.{RpcAddress, RpcCallContext}

import scala.concurrent.Promise

private[netty] abstract class NettyRpcCallContext(val senderAddress: RpcAddress)
  extends RpcCallContext {

  override def reply(response: Any): Unit = {
    send (response)
  }

  override def sendFailure(e: Throwable): Unit = {
    send (RpcFailure (e))
  }

  protected def send(message: Any): Unit
}

private[netty] class LocalNettyRpcCallContext(
    senderAddress: RpcAddress,
    p: Promise[Any]) extends NettyRpcCallContext(senderAddress) {

  override protected def send(message: Any): Unit = {
    p.success(message)
  }
}

private[netty] class RemoteNettyRpcCallContext(
    nettyEnv: NettyRpcEnv,
    callback: RpcResponseCallback,
    senderAddress: RpcAddress)
  extends NettyRpcCallContext(senderAddress) {

  override protected def send(message: Any): Unit = {
    val reply = nettyEnv.serialize(message)
    callback.onSuccess(reply)
  }
}
