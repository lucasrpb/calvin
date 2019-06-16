import java.util

import calvin.protocol._
import com.google.protobuf.any.Any
import com.twitter.util.Promise
import io.netty.buffer.{ByteBuf, ByteBufUtil}
import io.netty.channel.ChannelHandlerContext
import io.netty.handler.codec.{MessageToMessageDecoder, MessageToMessageEncoder}

package object calvin {

  val TIMEOUT = 400L

  case class Transaction(id: String, var keys: Seq[String], var tmp: Long){
    val p = Promise[Response]()
  }

  final class CommandEncoder extends MessageToMessageEncoder[Command] {
    override def encode(ctx: ChannelHandlerContext, msg: Command, out: util.List[AnyRef]): Unit = {

      val buf = ctx.alloc().buffer().retain()

      msg match {
        case cmd: Test => out.add(buf.writeBytes(Any.pack(cmd).toByteArray))
        case cmd: Enqueue => out.add(buf.writeBytes(Any.pack(cmd).toByteArray))
        case cmd: Release => out.add(buf.writeBytes(Any.pack(cmd).toByteArray))
        case cmd: Response => out.add(buf.writeBytes(Any.pack(cmd).toByteArray))
      }

      buf.release()
    }
  }

  final class CommandDecoder extends MessageToMessageDecoder[ByteBuf] {
    override def decode(ctx: ChannelHandlerContext, msg: ByteBuf, out: util.List[AnyRef]): Unit = {

      val bytes = ByteBufUtil.getBytes(msg).array
      val p = Any.parseFrom(bytes)

      p match {
        case _ if p.is(Test) => out.add(p.unpack(Test))
        case _ if p.is(Enqueue) => out.add(p.unpack(Enqueue))
        case _ if p.is(Release) => out.add(p.unpack(Release))
        case _ if p.is(Response) => out.add(p.unpack(Response))
      }

    }
  }

}
