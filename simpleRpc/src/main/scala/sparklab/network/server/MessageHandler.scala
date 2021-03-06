package sparklab.network.server

import sparklab.network.protocol.Message

abstract class MessageHandler[T <: Message] {
  def handle(message: T)

  def channelActive()

  def exceptionCaught(cause: Throwable)

  def channelInactive()
}
