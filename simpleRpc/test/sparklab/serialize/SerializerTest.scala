package sparklab.serialize

import org.scalatest.FunSuite

class SerializerTest extends FunSuite {
    test("JavaSerializer instances are serializable") {
      val serializer = new JavaSerializer()
      val instance = serializer.newInstance()
      val obj = instance.deserialize[Int](instance.serialize(1))
      println(obj)
      // enforce class cast
      obj.getClass
    }

    test("Deserialize object containing a primitive Class as attribute") {
      val serializer = new JavaSerializer()
      val instance = serializer.newInstance()
      val obj = instance.deserialize[ContainsPrimitiveClass](instance.serialize(
        new ContainsPrimitiveClass()))
      // enforce class cast
      obj.getClass
    }
  }

  private class ContainsPrimitiveClass extends Serializable {
    val intClass = classOf[Int]
    val longClass = classOf[Long]
    val shortClass = classOf[Short]
    val charClass = classOf[Char]
    val doubleClass = classOf[Double]
    val floatClass = classOf[Float]
    val booleanClass = classOf[Boolean]
    val byteClass = classOf[Byte]
    val voidClass = classOf[Void]
  }
