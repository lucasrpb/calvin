package calvin

import java.net.InetSocketAddress
import com.twitter.util.Await

object Server {

  def main(args: Array[String]): Unit = {

    val port = args(0)

    val service = new Transactor()

    val server = TransactorServer.Server().serve(new InetSocketAddress("localhost", port.toInt)
      , service)

    Await.result(server)

  }

}