package sparklab.util

import java.nio.{ByteBuffer, MappedByteBuffer}

import sun.nio.ch.DirectBuffer

object StorageUtils {
  def dispose(buffer: ByteBuffer): Unit = {
    if (buffer != null && buffer.isInstanceOf[MappedByteBuffer]) {
      // logTrace(s"Unmapping $buffer")
      if (buffer.asInstanceOf[DirectBuffer].cleaner() != null) {
        buffer.asInstanceOf[DirectBuffer].cleaner().clean()
      }
    }
  }

}
