package sparklab.rpc

import sparklab.rpc.netty.NettyRpcEnvFactory

import scala.concurrent.{Await, Future}

object RpcEnv {

  def create(
      name: String,
      host: String,
      port: Int,
      clientMode: Boolean = false): RpcEnv = {
    create (name, host, host, port, clientMode)
  }

  def create(
      name: String,
      bindAddress: String,
      advertiseAddress: String,
      port: Int,
      clientMode: Boolean): RpcEnv = {
    // 1. create the config file
    val config = RpcEnvConfig (name, bindAddress, advertiseAddress, port,
      clientMode)
    // 2. use the nettyrpcenvfactory to create a netty rpcenv
    new NettyRpcEnvFactory ().create (config)
  }
}

abstract class RpcEnv {

  // Return RpcEndpointRef of the registered RpcEndPoint.
  def endpointRef(endpoint: RpcEndpoint): RpcEndpointRef

  // Return the address that RpcEnv is listening to.
  def address: RpcAddress

  /**
   * Register a [[RpcEndpoint]] with a name and return its [[RpcEndpointRef]]. [[RpcEnv]] does not
   * guarantee thread-safety.
   */
  def setupEndpoint(name: String, endpoint: RpcEndpoint): RpcEndpointRef

  def setupEndpointRef(address: RpcAddress, endpointName: String): RpcEndpointRef = {
    setupEndpointRefByURI(RpcEndpointAddress(address, endpointName).toString)
  }

  def asyncSetupEndpointRefByURI(uri: String): Future[RpcEndpointRef]

  // a block operation
  def setupEndpointRefByURI(uri: String): RpcEndpointRef = {
    val timeout = RpcTimeout(120)
    timeout.awaitResult(asyncSetupEndpointRefByURI(uri))
  }

  // stop the RpcEndpoint specified by 'endpoint'
  def stop(endpoint: RpcEndpointRef): Unit

  // shutdown this RpcEnv
  def shutdown(): Unit

  def awaitTermination(): Unit
}

case class RpcEnvConfig(
    name: String,
    bindAddress: String,
    advertiseAddress: String,
    port: Int,
    clientMode: Boolean)