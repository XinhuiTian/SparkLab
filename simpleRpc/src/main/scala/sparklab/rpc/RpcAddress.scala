package sparklab.rpc

case class RpcAddress(host: String, port: Int) {

  /** Returns a string in the form of "spark://host:port". */
  // def toSparkURL: String = "spark://" + hostPort

  override def toString: String = hostPort

  def hostPort: String = host + ":" + port
}

object RpcAddress {
  /** Return the [[RpcAddress]] represented by `uri`. */
  def fromURIString(uri: String): RpcAddress = {
    val uriObj = new java.net.URI (uri)
    RpcAddress (uriObj.getHost, uriObj.getPort)
  }
}
