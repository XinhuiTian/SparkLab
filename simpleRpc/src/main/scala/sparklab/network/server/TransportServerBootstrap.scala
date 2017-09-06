package sparklab.network.server

import io.netty.channel.Channel

/**
 * A bootstrap which is executed on a TransportServer's client channel once a client connects
 * to the server. This allows customizing the client channel to allow for things such as SASL
 * authentication.
 */
trait TransportServerBootstrap {
  /**
   * Customizes the channel to include new features, if needed.
   *
   * @param channel    The connected channel opened by the client.
   * @param rpcHandler The RPC handler for the server.
   * @return The RPC handler to use for the channel.
   */
  def doBootstrap(channel: Channel, rpcHandler: RpcHandler): RpcHandler
}
