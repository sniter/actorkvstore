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
    scheduleOnce(RetryPersist)
  }

  def receive = {
      case JoinedPrimary   => 
        logMsg("[LEADER] Starting ...")
        role = "Leader"
        createReplicator(self)
        scheduleOnce(VerifyPendingReplica)
        context.become(leader orElse replication orElse replica orElse persistance)
      case JoinedSecondary => 
        logMsg("[REPLICA] Starting ... ")
        role = "Replica"
        context.become(replica orElse persistance)
  }

  def currentTimestamp: Long = Instant.now.toEpochMilli

  def updateKv(key: String, valueOpt: Option[String]): Unit =
    kv = valueOpt.fold {
      kv - key
    }{ value =>
      kv + (key -> value)
    } 
  
  def persist(msg: Snapshot, set: Boolean = false): Unit = 
    val persistMsg = Persist(msg.key, msg.valueOption, msg.seq)
    persistence ! persistMsg
    val expected = Persisted(msg.key, msg.seq)
    if (set){
      logMsg(s"[PERSIST] ${msg}")
      pending += expected -> (
        persistMsg, 
        currentTimestamp, 
        sender
      )
      updateKv(msg.key, msg.valueOption)
      oldSeq = msg.seq
    } else {
      logMsg(s"[PERSIST*] ${msg}")
    }
  
 
  def replicate(key: String, valueOpt: Option[String], id:Long): Unit = 
    replicate(Replicate(key, valueOpt, id))
    
  def replicate(msg: Replicate): Unit = 
    val expected = Replicated(msg.key, msg.id)
    if (!pendingReplications.contains(expected)){
      updateKv(msg.key, msg.valueOption)
      pendingReplications += expected -> (sender, msg, currentTimestamp, replicators.toSet)
      logMsg(s"Ticket: ${pendingReplications(expected)}")
      replicators foreach { replicator =>
        replicator ! msg
      }
    }

  def createReplicator(actor: ActorRef): Unit =
    val replicator_suffix: String = 
      if(actor != self)
        "follower-" + random.alphanumeric.take(4).toSeq.mkString("")
      else
        "leader"
    
    val replicator = context.actorOf(Replicator.props(actor), s"replicator-${replicator_suffix}")
    logMsg(s"[REPLICATOR] Starting replicator <${replicator}> for <${actor}>")
    secondaries += (actor -> replicator)
    replicators += replicator
    if (actor != self){
      context.watch(actor)
      restoreReplicaState(actor)
    }

  def stopReplicator(replicaUnit: ActorRef): ActorRef = {
    val replicator = secondaries(replicaUnit)
    secondaries -= replicaUnit
    replicators -= replicator
    context.stop(replicator)
    logMsg(s"[REPLICATOR] Stopping replicator <${replicator}> for <${replicaUnit}>")
    replicator
  }

  def stopReplication(replicaUnit: ActorRef): Unit = 
    pendingReplications.foreach{ 
        case (key, value @ (_, msg, _, replicas)) if replicas contains replicaUnit =>
          
          if(replicas.size > 1){
            logMsg(s"[REPLICATION] Stopping replication ${msg} for ${replicaUnit}")
            pendingReplications += key -> value.copy(_4 = replicas - replicaUnit)
          } else {
            logMsg(s"[REPLICATION*] Stopping replication ${msg}")
            pendingReplications -= key
          }
        case (key, value @ (_, _, _, replicas)) =>
    }
  
  def refreshReplicas(replicas: Set[ActorRef]): Unit = 
    val deadReplicas = secondaries.keySet -- replicas
    val newReplicas = replicas -- secondaries.keySet
    val oldReplicators = deadReplicas.toSeq.map(secondaries.apply)
    oldReplicators.foreach(stopReplication)
    deadReplicas.foreach(stopReplicator)
    newReplicas.foreach(createReplicator)
    

  def isValidSeq(seq: Long): Long =
    math.signum(seq - 1L - oldSeq)

  def restoreReplicaState(replica: ActorRef): Unit = 
    kv.zipWithIndex.foreach{
      case ((key, value), idx) => 
        replicate(key, Some(value), idx)
    }

  // override def logMsg(msg: String): Unit = ()

  /* TODO Behavior for  the leader role. */
  val leader: Receive = 
    case Replicas(replicas) =>
      logMsg(s"${role}.Replicas: ${replicas}")
      refreshReplicas(replicas)

    case Insert(key, value, id) => 
      logMsg(s"${role}.Insert: ${key} -> ${id} = ${value} @ ${sender}")
      replicate(key, Some(value), id)

    case Remove(key, id) => 
      logMsg(s"${role}.Remove: ${key} -> ${id}")
      replicate(key, None, id)

    case getRequest @ Get(key, id) if secondaries.isEmpty =>
      logMsg(s"${role}.Get[Leader]: ${key} -> ${id}")
      sender ! GetResult(key, kv.get(key), id)

    case getRequest @ Get(key, id) =>
      var keys = secondaries.keys
      val replicaUnit = random.shuffle(keys).head
      
      if (replicaUnit == self){
        logMsg(s"${role}.Get[Replica @Leader]: ${key} -> ${id}")
        sender ! GetResult(key, kv.get(key), id)
      }else{
        logMsg(s"${role}.Get[Replica ${replicaUnit}]: ${key} -> ${id}")
        replicaUnit forward getRequest
      }
    case Terminated(replicaUnit) if secondaries.contains(replicaUnit) => 
      stopReplicator(replicaUnit)
 
  val replication: Receive = 
    case done @ Replicated(key, id) if pendingReplications.contains(done) => 
      logMsg(s"${role}.Replicated: ${key} -> ${id} @ ${sender}")
      val (recipient, msg, created, pendingReplicas) = pendingReplications(done)
      if (pendingReplicas.size <= 1) {
        recipient ! OperationAck(id)
        pendingReplications -= done
        logMsg(s"[REPLICATED] From ${sender}\nClosing ticket ${msg}")
      } else {
        pendingReplications += done -> (recipient, msg, currentTimestamp, pendingReplicas - sender)
        logMsg(s"[REPLICATED] From ${sender}\nWaiting next replicated for ticket ${msg} from:\n${pendingReplicas.mkString("\n")}")
      }
    case VerifyPendingReplica =>
      if (pendingReplications.nonEmpty){
        logMsg(s"Pending persist:\n${pendingReplications.toSeq.map(v => v._1 -> v._2._4).mkString("\n")}\n")
        val now = currentTimestamp
        pendingReplications.foreach{ 
          case (expectedResponse, (recipient, msg, created, replicaUnits)) =>
            if ((now - created) > 1.second.toMillis) {
              logMsg(s"${role}.VerifyPendingReplica: Drop replica ${msg}")
              recipient ! OperationFailed(expectedResponse.id)
              pendingReplications -= expectedResponse
            } else {
              logMsg(s"${role}.VerifyPendingReplica: Retry replica ${msg} @ ${replicaUnits}")
              replicate(msg)
            }
        }
      }
      scheduleOnce(VerifyPendingReplica)

  val persistance: Receive = 
    case persisted @ Persisted(key, seq) if pending.contains(persisted) =>
      logMsg(s"${role}.Persisted: ${key} -> ${seq} from ${sender}")
      val (_, created, recipient) = pending(persisted)
      recipient ! SnapshotAck(key, seq)
      pending -= persisted
    case RetryPersist  =>
      if (pending.nonEmpty){
        logMsg(s"Pending persist:\n${pending.toSeq.map(v => v._1 -> v._2._3).mkString("\n")}\n")
        pending.foreach{
          case (_, (persist, _, _)) => 
            logMsg(s"${role}.RetryPersist: Resending message ${persist} ...")
            persistence ! persist
        }
      }
      scheduleOnce(RetryPersist)


  /* TODO Behavior for the replica role. */
  val replica: Receive = 
    case shot @ Snapshot(key, valueOpt, seq) if pending.contains(Persisted(key,seq)) =>
      logMsg(s"${role}.Snapshot[retry]: ${key} -> ${seq} = ${valueOpt}")
      persist(shot)
    case shot @ Snapshot(key, valueOpt, seq) =>
      isValidSeq(seq) match {
        case 0L =>
          logMsg(s"${role}.Snapshot: ${key} -> ${seq} = ${valueOpt}")
          persist(shot, set=true)
        case -1L =>
          logMsg(s"${role}.Snapshot[too old]: ${key} -> ${seq} = ${valueOpt}")
          sender ! SnapshotAck(key, seq)
        case 1L =>
          logMsg(s"${role}.Snapshot[too new]: ${key} -> ${seq} = ${valueOpt}")
          // Nothing happens
      }
    case Get(key, id) =>
      logMsg(s"${role}.Get: ${key} -> ${id} = ${kv.get(key)}")
      sender ! GetResult(key, kv.get(key), id)


