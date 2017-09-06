package sparklab.util

import java.io.IOException

import scala.util.control.NonFatal

object CommonUtils {
  def tryOrIOException[T](block: => T): T = {
    try {
      block
    } catch {
      case e: IOException =>
        // logError("Exception encountered", e)
        throw e
      case NonFatal(e) =>
        // logError("Exception encountered", e)
        throw new IOException(e)
    }
  }

}
