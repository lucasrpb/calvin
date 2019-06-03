import java.util.concurrent.ThreadLocalRandom

import scala.collection.concurrent.TrieMap
import scala.concurrent.Promise

package object calvin {

  val TIMEOUT = 700L

  trait Command

  case class Transaction(id: String, var keys: Seq[String], var tmp: Long){
    val p = Promise[Boolean]()
  }

  def latency(): Unit = {
    Thread.sleep(ThreadLocalRandom.current().nextLong(5L, 20L))
  }

  val transactions = TrieMap[String, Transaction]()
  val sequencers = TrieMap[String, Transactor]()

}
