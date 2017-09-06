package sparklab.deploy

import java.text.SimpleDateFormat
import java.util.Date

import sparklab.deploy.DeployMessages.{RegisteredWorker, RegisterWorker, RegisterWorkerResponse}

import scala.util.Success
import scala.util.Failure
import sparklab.rpc.{RpcAddress, RpcEndpointRef, RpcEnv, ThreadSafeRpcEndpoint}
import sparklab.rpc.netty.{OneWayInboxMessage, RequestMessage}
import sparklab.util.ThreadUtils

class Worker(
    override val rpcEnv: RpcEnv,
    masterRpcAddress: RpcAddress)
  extends ThreadSafeRpcEndpoint {

  private val host = rpcEnv.address.host
  private val port = rpcEnv.address.port

  private val workerId = generateWorkerId()

  override def onStart(): Unit = {
    println("I am started now!")

    registerMaster()
  }

  private def registerMaster(): Unit = {
    val masterClient = rpcEnv.setupEndpointRef(masterRpcAddress, "Master")
    masterClient.ask[RegisterWorkerResponse](RegisterWorker(workerId, host, port, self), 10)
      .onComplete {
      case Success(_) => println("Get the register response")
      case Failure(e) => println("Failed on register the worker", e)
    }(ThreadUtils.sameThread)
  }

  private def createDateFormat = new SimpleDateFormat("yyyyMMddHHmmss")

  private def generateWorkerId(): String = {
    "worker-%s-%s-%d".format(createDateFormat.format(new Date), host, port)
  }
}

object Worker {
  val SYSTEM_NAME = "sparkWorker"
  val ENDPOINT_NAME = "Worker"

  def main(argStrings: Array[String]) {

    val rpcEnv = startRpcEnvAndEndpoint ("localhost", 18080, "")
    rpcEnv.awaitTermination ()
  }

  def startRpcEnvAndEndpoint(
      host: String,
      port: Int,
      masterUrl: String): RpcEnv = {

    // The LocalSparkCluster runs multiple local sparkWorkerX RPC Environments
    val systemName = SYSTEM_NAME
    val rpcEnv = RpcEnv.create (systemName, host, port)
    val masterAddress = ""
    rpcEnv.setupEndpoint (ENDPOINT_NAME, new Worker (rpcEnv,
      new RpcAddress ("localhost", 9090)))
    rpcEnv
  }
}
