package sparklab.util

import java.net.BindException

object NetworkUtils {
  def startServiceOnPort[T](
      startPort: Int,
      startService: Int => (T, Int),
      serviceName: String = ""): (T, Int) = {

    val serviceString = if (serviceName.isEmpty) "" else s" '$serviceName'"

    // fixed value currently
    val maxRetries = 5

    for (offset <- 0 to maxRetries) {
      val tryPort = if (startPort == 0) {
        startPort
      } else {
        ((startPort + offset - 1024) % (65536 - 1024)) + 1024
      }

      try {
        val (service, port) = startService (tryPort)
        return (service, port)
      } catch {
        case e: Exception =>
          if (offset >= maxRetries) {
            val exceptionMessage = s"Over the max retries"
            val exception = new BindException (exceptionMessage)

            exception.setStackTrace (e.getStackTrace)
            throw exception
          }
      }
    }
    throw new Exception (s"Failed to start service on port $startPort")
  }

}
