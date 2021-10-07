package kvstore

import akka.actor.{ OneForOneStrategy, PoisonPill, Props, SupervisorStrategy, Terminated, ActorRef, Actor, actorRef2Scala }
import akka.event.Logging
import akka.pattern.{ ask, pipe }
import akka.util.Timeout
import scala.concurrent.duration.*
import scala.util.Random
import kvstore.Arbiter.*

object Replica:
  sealed trait Operation:
    def key: String
    def id: Long
  case class Insert(key: String, value: String, id: Long) extends Operation
  case class Remove(key: String, id: Long) extends Operation
  case class Get(key: String, id: Long) extends Operation

  sealed trait OperationReply
  case class OperationAck(id: Long) extends OperationReply
  case class OperationFailed(id: Long) extends OperationReply
  case class GetResult(key: String, valueOption: Option[String], id: Long) extends OperationReply

  def props(arbiter: ActorRef, persistenceProps: Props): Props = Props(Replica(arbiter, persistenceProps))

class Replica(val arbiter: ActorRef, persistenceProps: Props) extends Actor with AkkaHelpers:
  import Replica.*
  import Replicator.*
  import Persistence.*
  import context.dispatcher
  
  /*
   * The contents of this actor is just a suggestion, you can implement it in any way you like.
   */

  var kv = Map.empty[String, String]
  // a map from secondary replicas to replicators
  var secondaries = Map.empty[ActorRef, ActorRef]
  // the current set of replicators
  var replicators = Set.empty[ActorRef]
  val random = Random()
  var oldSeq = -1L
  var pending = Map.empty[Persisted, (Persist, ActorRef)]

  override def preStart(): Unit = {
    arbiter ! Join
  }

  def receive = {
    case JoinedPrimary   => context.become(leader)
    case JoinedSecondary => 
      scheduleOnce(RetryPersist)
      context.become(replica)
  }

  /* TODO Behavior for  the leader role. */
  val leader: Receive =
    case Replicas(replicas) =>
      ((replicas -- secondaries.keySet) - self).foreach{ replica =>
        val replicator = context.actorOf(Replicator.props(replica))
        context.watch(replica)
        secondaries = secondaries + (replica -> replicator)
        replicators = replicators + replicator
      }
    case Insert(key, value, id) =>
      kv = kv + (key -> value)
      replicators foreach { replicator =>
        replicator ! Replicate(key, Some(value), id)
      }
      sender ! OperationAck(id)
    case Remove(key, id) =>
      kv = kv - key
      replicators foreach { replicator =>
        replicator ! Replicate(key, None, id)
      }
      sender ! OperationAck(id)
    case op @ Get(key, id) =>
      if (secondaries.nonEmpty){
        var keys = secondaries.keys
        val replica = random.shuffle(keys).head
        if (replica == self)
          sender ! GetResult(key, kv.get(key), id)
        secondaries(keys.head) forward op
      } else {
        sender ! GetResult(key, kv.get(key), id)
      }
    case Terminated(replica) if secondaries.contains(replica) =>
      val replicator = secondaries(replica)
      secondaries = secondaries - replica
      replicators = replicators - replicator
      context.stop(replicator)
    case _ =>

  /* TODO Behavior for the replica role. */
  val replica: Receive = {
    val persistence = context.actorOf(persistenceProps)
    {
      case Snapshot(key, valueOpt, seq) if seq - 1L == oldSeq =>
        oldSeq = seq
        valueOpt.fold {
          kv = kv - key
        }{ value =>
          kv = kv + (key -> value)
        }
        val newPersist = Persist(key, valueOpt, seq)
        persistence ! newPersist
        //replicator ! SnapshotAck(key, seq)
        pending = pending + (Persisted(key, seq) -> (newPersist, sender))
      case Snapshot(key, valueOpt, seq) if seq - 1L <= oldSeq =>
        sender ! SnapshotAck(key, seq)
      case Snapshot(key, valueOpt, seq) if seq - 1L > oldSeq =>
        // Nothing happens
      case persisted @ Persisted(key, seq) if pending.contains(persisted) =>
        val (_, recipient) = pending(persisted)
        recipient ! SnapshotAck(key, seq)
        pending = pending - persisted
      case RetryPersist if pending.nonEmpty =>
        pending.foreach{
          case (_, (persist, _)) => persistence ! persist
        }
        scheduleOnce(RetryPersist)
      case Get(key, id) =>
        sender ! GetResult(key, kv.get(key), id)
      case msg =>
        ()
    }
  }

  // def persisting(replicator: ActorRef): Receive = {
  //   case Persisted(key, seq) =>
  //     replicator ! SnapshotAck(key, seq)
  //     context.become(replica, discardOld=true)
  //   case Get(key, id) =>
  //     sender ! GetResult(key, kv.get(key), id)
  // }


