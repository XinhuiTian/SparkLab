package sparklab.network.server

import java.nio.ByteBuffer

import sparklab.network.client.{RpcResponseCallback, TransportClient}

object RpcHandler {
  final val ONE_WAY_CALLBACK = new OneWayRpcCallBack ()
}

abstract class RpcHandler {
  def receive(
      client: TransportClient,
      message: ByteBuffer,
      callback: RpcResponseCallback)


  def receive(client: TransportClient, message: ByteBuffer): Unit = {
    receive (client, message, RpcHandler.ONE_WAY_CALLBACK)
  }
}

case class OneWayRpcCallBack() extends RpcResponseCallback {

  // final val logger = LoggerFactory.getLogger (OneWayRpcCallBack.getClass)

  override def onSuccess(response: ByteBuffer): Unit = {
    // logger.warn ("Response provided for one-way RPC")
  }

  override def onFailure(e: Throwable): Unit = {
    // logger.error ("Error response provided for one-way RPC")
  }
}
