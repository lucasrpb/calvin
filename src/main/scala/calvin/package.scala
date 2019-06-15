import java.nio.CharBuffer
import java.nio.charset.StandardCharsets.UTF_8
import java.util

import calvin.protocol.{Enqueue, Release}
import com.google.protobuf.any.Any
import io.netty.buffer.{ByteBuf, ByteBufUtil}
import io.netty.channel.ChannelHandlerContext
import io.netty.handler.codec.{MessageToMessageDecoder, MessageToMessageEncoder}

import scala.concurrent.Promise

package object calvin {

  val TIMEOUT = 700L

  case class Transaction(id: String, var keys: Seq[String], var tmp: Long){
    val p = Promise[Boolean]()
  }

  final class CommandEncoder extends MessageToMessageEncoder[Command] {
    override def encode(ctx: ChannelHandlerContext, msg: Command, out: util.List[AnyRef]): Unit = {

      val buf = ctx.alloc().buffer().retain()

      msg match {
        case cmd: Enqueue =>

          //val packed = Any.pack(cmd).toByteString
          //out.add(ByteBufUtil.encodeString(ctx.alloc, CharBuffer.wrap(packed.toStringUtf8), UTF_8))

          out.add(buf.writeBytes(Any.pack(cmd).toByteArray))

        case cmd: Release =>

         // val packed = Any.pack(cmd).toByteString
         // println("result: ", Enqueue.parseFrom(packed.toStringUtf8.getBytes))

          //out.add(ByteBufUtil.encodeString(ctx.alloc, CharBuffer.wrap(packed.toStringUtf8), UTF_8))

          out.add(buf.writeBytes(Any.pack(cmd).toByteArray))
        case _ =>
      }
    }
  }

  final class CommandDecoder extends MessageToMessageDecoder[ByteBuf] {
    override def decode(ctx: ChannelHandlerContext, msg: ByteBuf, out: util.List[AnyRef]): Unit = {

      /*val str = msg.toString(UTF_8)

      val p = Any.parseFrom(str.getBytes)

      p match {
        case _ if p.is(Enqueue) => out.add(Enqueue.parseFrom(str.getBytes))
        case _ if p.is(Release) => out.add(Release.parseFrom(str.getBytes))
        case _ =>
      }*/

      val bytes = ByteBufUtil.getBytes(msg).array
      val p = Any.parseFrom(bytes)

      p match {
        case _ if p.is(Enqueue) => out.add(Enqueue.parseFrom(bytes))
        case _ if p.is(Release) => out.add(Release.parseFrom(bytes))
        case _ =>
      }

    }
  }

}
