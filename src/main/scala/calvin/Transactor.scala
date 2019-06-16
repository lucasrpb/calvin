package calvin

import java.util.{Timer, TimerTask}
import java.util.concurrent.ConcurrentLinkedQueue
import calvin.protocol._
import com.twitter.finagle.Service
import com.twitter.util.Future
import scala.collection.concurrent.TrieMap

class Transactor extends Service[Command, Command] {

  val batch = new ConcurrentLinkedQueue[Transaction]()
  var running = TrieMap[String, Transaction]()

  val timer = new Timer()

  timer.scheduleAtFixedRate(new TimerTask {
    override def run(): Unit = {

      var txs = Seq.empty[Transaction]
      val it = batch.iterator()

      while(it.hasNext){
        val t = it.next()
        txs = txs :+ t
      }

      val now = System.currentTimeMillis()

      running = running.filter { case (_, t) =>
        now - t.tmp < TIMEOUT
      }

      var keys = running.map(_._2.keys).flatten.toSeq

      txs.sortBy(_.id).foreach { t =>
        val elapsed = now - t.tmp

        if(elapsed >= TIMEOUT){
          batch.remove(t)
          t.p.setValue(Response(false))
        } else if(!t.keys.exists(keys.contains(_))) {

          batch.remove(t)
          keys = keys ++ t.keys

          t.p.setValue(Response(true))
        }
      }

    }
  }, 10L, 10L)

  def enqueue(cmd: Enqueue): Future[Command] = {

    val t = Transaction(cmd.id, cmd.keys, System.currentTimeMillis())

    if(batch.size() < 10000){
      batch.add(t)
      return t.p
    }

    t.p.setValue(Response(false))

    t.p
  }

  def release(id: String): Future[Command] = Future {
    running.remove(id)
    Response(true)
  }

  override def apply(request: Command): Future[Command] = {
    request match {
      case cmd: Enqueue => enqueue(cmd)
      case cmd: Release => release(cmd.id)
      case _ => Future.value(Test("other"))
    }
  }
}
