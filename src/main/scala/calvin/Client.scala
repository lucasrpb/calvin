package calvin

import java.util.UUID
import java.util.concurrent.ThreadLocalRandom

import com.twitter.finagle.Service
import com.twitter.finagle.client.StackClient
import com.twitter.finagle.dispatch.SerialClientDispatcher
import com.twitter.finagle.netty4.Netty4Transporter
import com.twitter.util._
import calvin.protocol._
import com.twitter.finagle.builder.ClientBuilder

import scala.collection.concurrent.TrieMap

object Client {

  case class Account(id: String, var balance: Int)

  val rand = ThreadLocalRandom.current()

  val mapping = Map(
    "0" -> 2551,
    "1" -> 2552
  )
  val transactors = TrieMap[String, Service[Command, Command]]()

  def createConnection(host: String, port: Int): Service[Command, Command] = {
    val addr = new java.net.InetSocketAddress(host, port)
    val transporter = Netty4Transporter.raw[Command, Command](TransactorClient.NullDelimiterPipeline, addr,
      StackClient.defaultParams)

    val bridge: Future[Service[Command, Command]] = transporter() map { transport =>
      new SerialClientDispatcher[Command, Command](transport)
    }

    (req: Command) => bridge flatMap { svc =>
      svc(req) //ensure svc.close()
    }
  }

  def lock(srv: Service[Command, Command], request: Command): Future[Response] = {
    srv.apply(request).map(_.asInstanceOf[Response])
  }

  def release(srv: Service[Command, Command], request: Command): Future[Response] = {
    srv.apply(request).map(_.asInstanceOf[Response])
  }

  def main(args: Array[String]): Unit = {

    val n = 2

    for(i<-0 until n){
      transactors.put(i.toString, createConnection("localhost", mapping(i.toString)))
    }

    var accounts = Seq.empty[Account]

    for(i<-0 until 100){
      val id = UUID.randomUUID.toString
      val balance = rand.nextInt(0, 1000)

      val account = Account(id, balance)

      accounts = accounts :+ account
    }

    def transact(keys: Seq[String]): Future[Boolean] = {
      val id = UUID.randomUUID.toString
      var requests = Map.empty[String, Enqueue]

      keys.foreach { k =>
        val p = (transactors.computeHash(k).abs % n).toString/*(k.toInt % n).toString*/

        requests.get(p) match {
          case Some(e) => e.addKeys(k)
          case None => requests = requests + (p -> Enqueue(id, Seq(k)))
        }
      }

      val start = System.currentTimeMillis()
      implicit val timer = new JavaTimer()

      val ptimeout = new Promise[Seq[Response]]()
      timer.schedule(Duration.fromMilliseconds(TIMEOUT)){
        ptimeout.setValue(Seq(Response(false)))
        //ptimeout.raise(new TimeoutException("whoops!"))
      }

      val locks = Future.traverseSequentially(requests.toSeq){case (p, e) => lock(transactors(p), e)}

      locks.or(ptimeout).map { reads =>
        !reads.exists(_.ok == false)
      }.handle { case e: Throwable =>
        println(s"exception ${e}")
        false
      }.map { ok =>

        requests.foreach { case (p,_) => release(transactors(p), Release(id))}
        timer.stop()

        val elapsed = System.currentTimeMillis() - start
        println(s"transaction ${id} done => ${ok} elapsed: ${elapsed}ms")

        ok
      }
    }

    var tasks = Seq.empty[Seq[String]]

    val iter = 1000

    for(i<-0 until iter){
      val i0 = rand.nextInt(0, n).toString
      val i1 = rand.nextInt(0, n).toString

      tasks = tasks :+ Seq(i0, i1)
    }

    val start = System.currentTimeMillis()
    val results = Await.result(Future.traverseSequentially(tasks)(transact(_))/*, Duration.fromSeconds(10)*/)
    val elapsed = System.currentTimeMillis() - start

    val size = results.length
    val hits = results.count(_ == true)
    val rate = hits*100/size

    val rps = (size * 1000)/elapsed

    println(s"n: ${size} hits: ${hits} rate: ${rate}% rps: ${rps}\n")

    Await.all(transactors.map(_._2.close()).toSeq: _*)
  }

}
