import java.util
import java.util.concurrent.ThreadLocalRandom

import calvin.protocol.{Enqueue, Release}
import io.netty.buffer.{ByteBuf, ByteBufUtil}
import io.netty.channel.ChannelHandlerContext
import io.netty.handler.codec.{MessageToMessageDecoder, MessageToMessageEncoder}

import scala.collection.concurrent.TrieMap
import scala.concurrent.Promise

package object calvin {

  val TIMEOUT = 700L

  case class Transaction(id: String, var keys: Seq[String], var tmp: Long){
    val p = Promise[Boolean]()
  }

  final class CommandEncoder extends MessageToMessageEncoder[Command] {
    override def encode(ctx: ChannelHandlerContext, msg: Command, out: util.List[AnyRef]): Unit = {
      val buf = ctx.alloc().directBuffer()

      msg match {
        case cmd: Enqueue => out.add(buf.writeBytes(cmd.toByteArray))
        case cmd: Release => out.add(buf.writeBytes(cmd.toByteArray))
        case _ =>
      }
    }
  }

  final class CommandDecoder extends MessageToMessageDecoder[ByteBuf] {
    override def decode(ctx: ChannelHandlerContext, msg: ByteBuf, out: util.List[AnyRef]): Unit = {

    }
  }

}
