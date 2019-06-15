package calvin

import java.util.concurrent.ThreadLocalRandom
import java.util.{Timer, TimerTask, UUID}

import calvin.protocol._
import com.google.protobuf.Descriptors.Descriptor
import com.google.protobuf.{Descriptors, DynamicMessage}
import com.google.protobuf.any.Any
import org.scalatest.FlatSpec

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future, Promise, TimeoutException}

class SerializationSpec extends FlatSpec {

  "money " should "be the same after transactions" in {

    val cmd1 = Enqueue("1", Seq("k1"))
    val cmd2 = Release("1")
    val cmd3 = Test("2")

    val p1 = Any.pack(cmd1)
    val b1 = p1.toByteArray

    val p11 = Any.parseFrom(b1)

   // println(com.google.protobuf.Any.parseFrom(b1).is(classOf[Enqueue]))

    println(p11.is(Enqueue))
    println(Enqueue.parseFrom(b1))

  }

}
