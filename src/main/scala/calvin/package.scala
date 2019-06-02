import scala.collection.concurrent.TrieMap
import scala.concurrent.Promise

package object calvin {

  val TIMEOUT = 700L

  trait Command

  case class Transaction(id: String, var keys: Seq[String], var tmp: Long){
    val p = Promise[Boolean]()
  }

  val transactions = TrieMap[String, Transaction]()
  val sequencers = TrieMap[String, Sequencer]()

}
