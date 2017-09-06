package sparklab.serialize

import java.io._
import java.nio.ByteBuffer

import sparklab.util.CommonUtils

import scala.reflect.ClassTag

class JavaSerializationStream(
    out: OutputStream, counterReset: Int, extraDebugInfo: Boolean)
  extends SerializationStream {
  private val objOut = new ObjectOutputStream (out)
  private var counter = 0

  /**
   * Calling reset to avoid memory leak:
   * http://stackoverflow.com/questions/1281549/memory-leak-traps-in-the-java-standard-api
   * But only call it every 100th time to avoid bloated serialization streams (when
   * the stream 'resets' object class descriptions have to be re-written)
   */
  def writeObject[T: ClassTag](t: T): SerializationStream = {
    try {
      objOut.writeObject (t)
    } catch {
      case e: NotSerializableException => println ("No Serial")
    }
    counter += 1
    if (counterReset > 0 && counter >= counterReset) {
      objOut.reset ()
      counter = 0
    }
    this
  }

  def flush() {
    objOut.flush ()
  }

  def close() {
    objOut.close ()
  }
}

class JavaDeserializationStream(in: InputStream, loader: ClassLoader)
  extends DeserializationStream {

  private val objIn = new ObjectInputStream (in) {
    override def resolveClass(desc: ObjectStreamClass): Class[_] =
      try {
        // scalastyle:off classforname
        Class.forName (desc.getName, false, loader)
        // scalastyle:on classforname
      } catch {
        case e: ClassNotFoundException =>
          JavaDeserializationStream.primitiveMappings.getOrElse (desc.getName, throw e)
      }
  }

  def readObject[T: ClassTag](): T = objIn.readObject ().asInstanceOf [T]

  def close() {
    objIn.close ()
  }
}

private object JavaDeserializationStream {
  val primitiveMappings = Map [String, Class[_]](
    "boolean" -> classOf [Boolean],
    "byte" -> classOf [Byte],
    "char" -> classOf [Char],
    "short" -> classOf [Short],
    "int" -> classOf [Int],
    "long" -> classOf [Long],
    "float" -> classOf [Float],
    "double" -> classOf [Double],
    "void" -> classOf [Void]
  )
}

class JavaSerializerInstance(
    counterReset: Int, extraDebugInfo: Boolean, defaultClassLoader: ClassLoader)
  extends SerializerInstance {

  override def serialize[T: ClassTag](t: T): ByteBuffer = {
    val bos = new ByteBufferOutputStream ()
    val out = serializeStream (bos)
    out.writeObject (t)
    out.close ()
    bos.toByteBuffer
  }

  override def serializeStream(s: OutputStream): SerializationStream = {
    new JavaSerializationStream (s, counterReset, extraDebugInfo)
  }

  override def deserialize[T: ClassTag](bytes: ByteBuffer): T = {
    val bis = new ByteBufferInputStream (bytes)
    val in = deserializeStream (bis)
    in.readObject ()
  }

  override def deserializeStream(s: InputStream): DeserializationStream = {
    new JavaDeserializationStream (s, defaultClassLoader)
  }

  override def deserialize[T: ClassTag](bytes: ByteBuffer, loader: ClassLoader): T = {
    val bis = new ByteBufferInputStream (bytes)
    val in = deserializeStream (bis, loader)
    in.readObject ()
  }

  def deserializeStream(s: InputStream, loader: ClassLoader): DeserializationStream = {
    new JavaDeserializationStream (s, loader)
  }
}

class JavaSerializer extends Serializer with Externalizable {
  private var counterReset = 100
  private var extraDebugInfo = true

  override def newInstance(): SerializerInstance = {
    val classLoader = defaultClassLoader.getOrElse (Thread.currentThread.getContextClassLoader)
    new JavaSerializerInstance (counterReset, extraDebugInfo, classLoader)
  }

  override def writeExternal(out: ObjectOutput): Unit = CommonUtils.tryOrIOException{
    out.writeInt (counterReset)
    out.writeBoolean (extraDebugInfo)
  }

  override def readExternal(in: ObjectInput): Unit = CommonUtils.tryOrIOException{
    counterReset = in.readInt ()
    extraDebugInfo = in.readBoolean ()
  }

}
