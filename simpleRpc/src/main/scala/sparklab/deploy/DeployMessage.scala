package sparklab.deploy

import sparklab.rpc.RpcEndpointRef

sealed trait DeployMessage extends Serializable {

}

object DeployMessages {

  case class RegisterWorker(
      id: String,
      host: String,
      port: Int,
      worker: RpcEndpointRef)
  extends DeployMessage {

  }

  case class Heartbeat(workerId: String, worker: RpcEndpointRef) extends DeployMessage

  sealed trait RegisterWorkerResponse

  case class RegisteredWorker(master: RpcEndpointRef, masterWebUiUrl: String) extends DeployMessage
    with RegisterWorkerResponse
}
