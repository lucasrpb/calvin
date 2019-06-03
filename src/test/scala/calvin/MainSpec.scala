package calvin

import java.util.{Timer, TimerTask, UUID}
import java.util.concurrent.ThreadLocalRandom

import org.scalatest.FlatSpec

import scala.collection.concurrent.TrieMap
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Await, Future, Promise, TimeoutException}
import scala.concurrent.duration._

class MainSpec extends FlatSpec {

  val rand = ThreadLocalRandom.current()

  case class Account(id: String, var balance: Int)

  "money " should "be the same after transactions" in {

    var accounts = Seq.empty[Account]

    val ns = 20

    for(i<-0 until ns){
      val id = i.toString
      sequencers.put(id, new Transactor(id))
    }

    val n = 100

    for(i<-0 until n){
      val id = UUID.randomUUID.toString
      //val id = i.toString
      val balance = rand.nextInt(0, 1000)
      val account = new Account(id, balance)

      accounts = accounts :+ account
    }

    def transaction(i0: Int, i1: Int): Future[Boolean] = {
      val id = UUID.randomUUID.toString

      val a0 = accounts(i0)
      val a1 = accounts(i1)

      val keys = Seq(a0.id, a1.id)

      var requests = Map[String, Transaction]()

      val tmp = System.currentTimeMillis()

      keys.foreach { k =>
        val s = (sequencers.computeHash(k).abs & (ns - 1)).toString

        requests.get(k) match {
          case None => requests = requests + (s -> Transaction(id, Seq(k), tmp))
          case Some(t) => t.keys = t.keys :+ k
        }
      }

      val ptmt = Promise[Seq[Boolean]]()
      val timer = new Timer()

      val locks = requests.map{case (s, t) => sequencers(s).offer(t)}.toSeq

      timer.schedule(new TimerTask {
        override def run(): Unit = {
          ptmt.failure(new TimeoutException())
        }
      }, TIMEOUT)

      val start = System.currentTimeMillis()

      Future.firstCompletedOf(Seq(Future.sequence(locks), ptmt.future)).map { acks =>
        if(!acks.contains(false)) {
          latency()
          true
        } else {
          false
        }
      }.recover {case _ =>
        false
      }.map { ok =>

        val elapsed = System.currentTimeMillis() - start

        requests.foreach { case (p, _) =>
          sequencers(p).release(id)
        }

        println(s"tx ${id} done -> ${ok} elapsed: ${elapsed}ms")

        ok
      }
    }

    var tasks = Seq.empty[Future[Boolean]]

    for(i<-0 until 1000){
      val i0 = rand.nextInt(0, n)
      val i1 = rand.nextInt(0, n)

      if(!i0.equals(i1)){
        tasks = tasks :+ transaction(i0, i1)
      }
    }

    val start = System.currentTimeMillis()
    val results = Await.result(Future.sequence(tasks), 10 seconds)
    val elapsed = System.currentTimeMillis() - start

    val size = results.length
    val hits = results.count(_ == true)
    val rate = hits*100/size

    val rps = (size * 1000)/elapsed

    println(s"n: ${size} hits: ${hits} rate: ${rate}% rps: ${rps}\n")

  }

}
