package sparklab.deploy

import sparklab.deploy.DeployMessages.{RegisteredWorker, RegisterWorker}
import sparklab.rpc.{RpcAddress, RpcCallContext, RpcEndpoint, RpcEnv}

class Master(
    override val rpcEnv: RpcEnv,
    address: RpcAddress
) extends RpcEndpoint {
  /**
   * The [[RpcEnv]] that this [[RpcEndpoint]] is registered to.
   */

  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case RegisterWorker(id, workerHost, workerPort, workerRef) =>
      context.reply(RegisteredWorker(self, "MasterUiUrl"))
  }
}

object Master {
  val SYSTEM_NAME = "sparkMaster"
  val ENDPOINT_NAME = "Master"

  def main(argStrings: Array[String]) {
    val (rpcEnv, _) = startRpcEnvAndEndpoint("localhost", 9090)
    rpcEnv.awaitTermination()
  }

  /**
   * Start the Master and return a three tuple of:
   *   (1) The Master RpcEnv
   *   (2) The web UI bound port
   *   (3) The REST server bound port, if any
   */
  def startRpcEnvAndEndpoint(
      host: String,
      port: Int): (RpcEnv, Int) = {
    val rpcEnv = RpcEnv.create(SYSTEM_NAME, host, port)
    val masterEndpoint = rpcEnv.setupEndpoint(ENDPOINT_NAME,
      new Master(rpcEnv, rpcEnv.address))
    (rpcEnv, port)
    // val portsResponse = masterEndpoint.askWithRetry[BoundPortsResponse](BoundPortsRequest)
    // (rpcEnv, portsResponse.webUIPort, portsResponse.restPort)
  }
}