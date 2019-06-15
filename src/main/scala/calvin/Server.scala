package calvin

import java.net.{InetAddress, InetSocketAddress}

import calvin.protocol.{Enqueue, Release}
import com.twitter.finagle.Service
import com.twitter.util.{Await, Future}

object Server {

  def main(args: Array[String]): Unit = {

    val port = args(0)

    val service = new Service[Command, Command] {
      override def apply(request: Command): Future[Command] = {

        request match {
          case cmd: Enqueue => println("received ", cmd.id)
          case cmd: Release => println("received ", cmd.id)
          case _ =>
        }

        Future.value(Release("world!"))
      }
    }

    val server = TransactorServer.Server().serve(new InetSocketAddress(InetAddress.getLoopbackAddress, port.toInt)
      , service)

    Await.result(server)

  }

}