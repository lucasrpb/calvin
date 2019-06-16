package calvin

import java.nio.CharBuffer
import java.nio.charset.StandardCharsets.UTF_8
import java.util

import io.netty.buffer.{ByteBuf, ByteBufUtil}
import io.netty.channel.{ChannelHandlerContext, ChannelPipeline}
import io.netty.handler.codec
import io.netty.handler.codec.Delimiters

object TransactorClient {

  val protocolLibrary = "string"

  object NullDelimiterPipeline extends (ChannelPipeline => Unit) {
    def apply(pipeline: ChannelPipeline): Unit = {
      // pipeline.addLast("stringEncoder", new StringEncoder(UTF_8))
      // pipeline.addLast("stringDecoder", new StringDecoder(UTF_8))

      pipeline.addLast("lineEncoder", new codec.MessageToMessageEncoder[ByteBuf] {
        override def encode(ctx: ChannelHandlerContext, msg: ByteBuf, out: util.List[AnyRef]): Unit = {

          //val bytes = ByteBufUtil.encodeString(ctx.alloc(), CharBuffer.wrap(msg.toString(UTF_8).concat("\n")), UTF_8)

          //val line = ByteBufUtil.encodeString(ctx.alloc(), CharBuffer.wrap("\n"), UTF_8)

          msg.retain()
          msg.writeBytes(Delimiters.nulDelimiter()(0))

          out.add(msg)
        }
      } )

      pipeline.addLast("commandEncoder", new CommandEncoder())
      pipeline.addLast("commandDecoder", new CommandDecoder())

    }
  }

}
