package sparklab.network.client

import java.nio.ByteBuffer

/**
 * Created by XinhuiTian on 17/6/9.
 * Callback for the result of a single RPC
 * This will be invoked once with either success or failure
 */
trait RpcResponseCallback {

  def onSuccess(response: ByteBuffer)

  def onFailure(e: Throwable)
}
