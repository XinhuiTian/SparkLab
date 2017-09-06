package sparklab.rpc

case class RpcEndpointAddress(val rpcAddress: RpcAddress, val name: String) {
  override val toString = if (rpcAddress != null) {
    s"simple://$name@${rpcAddress.host}:${rpcAddress.port}"
  } else {
    s"simple-client://$name"
  }

  def this(host: String, port: Int, name: String) = {
    this (RpcAddress (host, port), name)
  }
}

object RpcEndpointAddress {
  def apply(url: String): RpcEndpointAddress = {
    try {
      val uri = new java.net.URI (url)
      val host = uri.getHost
      val port = uri.getPort
      val name = uri.getUserInfo
      if (uri.getScheme != "simple" || // check the scheme
        host == null || // whether host is empty
        port < 0 || // invalid port
        name == null || //
        (uri.getPath != null && !uri.getPath.isEmpty) || // uri.getPath returns "" instead of null
        uri.getFragment != null ||
        uri.getQuery != null) {
        throw new Exception ("Invalid Spark URL: " + url)
      }
      new RpcEndpointAddress (host, port, name)
    } catch {
      case e: java.net.URISyntaxException =>
        throw new Exception ("Invalid Spark URL: " + url, e)
    }
  }
}