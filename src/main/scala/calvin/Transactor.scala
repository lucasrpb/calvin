package calvin

import java.util.{Timer, TimerTask}
import java.util.concurrent.ConcurrentLinkedQueue

import scala.collection.concurrent.TrieMap
import scala.concurrent.{ExecutionContext, Future}

class Transactor(val id: String)(implicit ec: ExecutionContext) {

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

      // Latency to save jobs executing...
      latency()

      txs.sortBy(_.id).foreach { t =>
        val elapsed = now - t.tmp

        if(elapsed >= TIMEOUT){
          batch.remove(t)
          t.p.success(false)
        } else if(!t.keys.exists(keys.contains(_))) {

          batch.remove(t)
          keys = keys ++ t.keys

          t.p.success(true)
        }
      }

    }
  }, 10L, 10L)

  def offer(t: Transaction): Future[Boolean] = {

    latency()

    if(batch.size() < 10000){
      batch.add(t)

      return t.p.future
    }

    t.p.success(false)

    t.p.future
  }

  def release(id: String): Future[Boolean] = Future {

    latency()

    running.remove(id)
    true
  }

}
