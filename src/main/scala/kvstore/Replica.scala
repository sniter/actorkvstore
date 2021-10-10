package kvstore

import akka.actor.{ OneForOneStrategy, PoisonPill, Props, SupervisorStrategy, Terminated, ActorRef, Actor, actorRef2Scala }
import akka.event.Logging
import akka.pattern.{ ask, pipe }
import akka.util.Timeout
import scala.concurrent.duration.*
import scala.util.Random
import java.time.Instant
import kvstore.Arbiter.*

object Replica:
  sealed trait Operation:
    def key: String
    def id: Long
  case class Insert(key: String, value: String, id: Long) extends Operation
  case class Remove(key: String, id: Long) extends Operation
  case class Get(key: String, id: Long) extends Operation

  case object VerifyPendingReplica

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
  type Created = Long
  var kv = Map.empty[String, String]
  // a map from secondary replicas to replicators
  var secondaries = Map.empty[ActorRef, ActorRef]
  // the current set of replicators
  var replicators = Set.empty[ActorRef]
  val random = Random()
  var oldSeq = -1L
  var pending = Map.empty[Persisted, (Persist, Created, ActorRef)]
  var pendingReplications = Map.empty[Replicated, (ActorRef, Replicate, Created, Set[ActorRef])]
  var persistence: ActorRef = _ 
  var role: String = _

  override def preStart(): Unit = {
    arbiter ! Join
    persistence = context.actorOf(persistenceProps)
  }

  def receive = {
      case JoinedPrimary   => 
        role = "Leader"
        createReplicator(self)
        scheduleOnce(VerifyPendingReplica)
        scheduleOnce(RetryPersist)
        context.become(leader orElse replication orElse replica orElse persistance)
      case JoinedSecondary => 
        role = "Replica"
        scheduleOnce(RetryPersist)
        context.become(replica orElse persistance)
  }

  def currentTimestamp: Long = Instant.now.toEpochMilli

  def updateKv(key: String, valueOpt: Option[String]): Unit =
    kv = valueOpt.fold {
      kv - key
    }{ value =>
      kv + (key -> value)
    } 

  def persist(key: String, valueOpt: Option[String], seq:Long): Unit = 
    val expected = Persisted(key, seq)
    if (!pending.contains(expected)){
      val newPersist = Persist(key, valueOpt, seq)
      persistence ! newPersist
      log.error(s"[MSG] ${newPersist}")
      pending += expected -> (newPersist, currentTimestamp, sender)
    }
 
  def replicate(key: String, valueOpt: Option[String], id:Long): Unit = 
    replicate(Replicate(key, valueOpt, id))
    
  def replicate(msg: Replicate): Unit = 
    val expected = Replicated(msg.key, msg.id)
    if (!pendingReplications.contains(expected)){
      updateKv(msg.key, msg.valueOption)
      pendingReplications += expected -> (sender, msg, currentTimestamp, replicators.toSet)
      log.error(s"Ticket: ${pendingReplications(expected)}")
      replicators foreach { replicator =>
        replicator ! msg
      }
    }

  def createReplicator(actor: ActorRef): Unit =
    val replicator = context.actorOf(Replicator.props(actor))
    secondaries += (actor -> replicator)
    replicators += replicator
    if (actor != self){
      context.watch(actor)
      restoreReplicaState(actor)
    }
  
  def refreshReplicas(replicas: Set[ActorRef]): Unit = 
    (replicas -- secondaries.keySet).foreach(createReplicator)

  def isValidSeq(seq: Long): Long =
    math.signum(seq - 1L - oldSeq)

  def restoreReplicaState(replica: ActorRef): Unit = 
    kv.zipWithIndex.foreach{
      case ((key, value), idx) => 
        replicate(key, Some(value), idx)
    }

  /* TODO Behavior for  the leader role. */
  val leader: Receive = 
    case Replicas(replicas) =>
      log.error(s"${role}.Replicas: ${replicas}")
      refreshReplicas(replicas)

    case Insert(key, value, id) => 
      log.error(s"${role}.Insert: ${key} -> ${id} = ${value} @ ${sender}")
      replicate(key, Some(value), id)
      // sender ! OperationAck(id)

    case Remove(key, id) => 
      log.error(s"${role}.Remove: ${key} -> ${id}")
      replicate(key, None, id)
      // sender ! OperationAck(id)

    case getRequest @ Get(key, id) if secondaries.isEmpty =>
      log.error(s"${role}.Get[Leader]: ${key} -> ${id}")
      sender ! GetResult(key, kv.get(key), id)

    case getRequest @ Get(key, id) =>
      var keys = secondaries.keys
      val replicaUnit = random.shuffle(keys).head
      
      if (replicaUnit == self){
        log.error(s"${role}.Get[Replica @Leader]: ${key} -> ${id}")
        sender ! GetResult(key, kv.get(key), id)
      }else{
        log.error(s"${role}.Get[Replica ${replicaUnit}]: ${key} -> ${id}")
        replicaUnit forward getRequest
      }
    case Terminated(replicaUnit) if secondaries.contains(replicaUnit) =>
      val replicator = secondaries(replicaUnit)
      secondaries -= replicaUnit
      replicators -= replicator
      context.stop(replicator)
 
  val replication: Receive = 
    case done @ Replicated(key, id) if pendingReplications.contains(done) => 
      log.error(s"${role}Replicated: ${key} -> ${id}")
      val (recipient, msg, created, pendingReplicas) = pendingReplications(done)
      if (pendingReplicas.size <= 1) {
        recipient ! OperationAck(id)
        pendingReplications -= done
      } else {
        pendingReplications += done -> (recipient, msg, currentTimestamp, pendingReplicas - sender)
      }
    case VerifyPendingReplica =>
      //log.error("VerifyPendingReplica...")
      val now = currentTimestamp
      pendingReplications.foreach{ 
        case (expectedResponse, (recipient, msg, created, numLeft)) =>
          if ((now - created) > 1.second.toMillis) {
            log.error(s"${role}.VerifyPendingReplica: Drop replica ${msg}")
            recipient ! OperationFailed(expectedResponse.id)
            pendingReplications -= expectedResponse
          } else {
            log.error(s"${role}.VerifyPendingReplica: Retry replica ${msg}")
            replicate(msg)
          }
      }
      scheduleOnce(VerifyPendingReplica)

  val persistance: Receive = 
    case persisted @ Persisted(key, seq) if pending.contains(persisted) =>
      log.error(s"${role}.Persisted: {} -> {}", key, seq)
      val (_, created, recipient) = pending(persisted)
      recipient ! SnapshotAck(key, seq)
      pending -= persisted
    case RetryPersist if pending.nonEmpty =>
      pending.foreach{
        case (_, (persist, _, _)) => 
          log.error(s"${role}.RetryPersist: Resending message {} ...", persist)
          persistence ! persist
      }
      scheduleOnce(RetryPersist)


  /* TODO Behavior for the replica role. */
  val replica: Receive = 
    case Snapshot(key, valueOpt, seq) if pending.contains(Persisted(key, seq)) =>
      // Do nothing
    case Snapshot(key, valueOpt, seq) =>
      isValidSeq(seq) match {
        case 0L =>
          log.error(s"${role}.Snapshot: {} {} {}", key, valueOpt, seq)
          updateKv(key, valueOpt)
          persist(key, valueOpt, seq)
          oldSeq = seq
        case -1L =>
          log.error(s"${role}.Snapshot[too old]: {} {} {}", key, valueOpt, seq)
          sender ! SnapshotAck(key, seq)
        case 1L =>
          log.error(s"${role}.Snapshot[too new]: {} {} {}", key, valueOpt, seq)
          // Nothing happens
      }
    case Get(key, id) =>
      log.error(s"${role}.Get: {} {} {}", key, kv.get(key), id)
      sender ! GetResult(key, kv.get(key), id)


