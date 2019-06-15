package calvin

import com.twitter.finagle.Service
import com.twitter.finagle.client.StackClient
import com.twitter.finagle.dispatch.SerialClientDispatcher
import com.twitter.finagle.netty4.Netty4Transporter
import com.twitter.util.{Await, Future}
import calvin.protocol._

object Client {

  def main(args: Array[String]): Unit = {

    val port = args(0)

    val addr = new java.net.InetSocketAddress("127.0.0.1", port.toInt)
    val transporter = Netty4Transporter.raw[Command, Command](TransactorClient.NoDelimStringPipeline, addr,
      StackClient.defaultParams)

    val bridge: Future[Service[Command, Command]] =
      transporter() map { transport =>
        new SerialClientDispatcher[Command, Command](transport)
      }

    val client = new Service[Command, Command] {
      def apply(req: Command) = bridge flatMap { svc =>
        svc(req) ensure svc.close()
      }
    }

    val start = System.currentTimeMillis()
    val r = Await.result(client(Release("hello")))
    val elapsed = System.currentTimeMillis() - start

    println(s"result ${r} elapsed ${elapsed}ms")

  }

}
