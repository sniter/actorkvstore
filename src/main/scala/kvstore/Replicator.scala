package kvstore

import akka.actor.Props
import akka.actor.Actor
import akka.actor.ActorRef
import akka.actor.actorRef2Scala
import akka.event.Logging
import scala.concurrent.duration.*
import java.time.Instant

object Replicator:
  case class Replicate(key: String, valueOption: Option[String], id: Long)
  case class Replicated(key: String, id: Long)
  
  case class Snapshot(key: String, valueOption: Option[String], seq: Long)
  case class SnapshotAck(key: String, seq: Long)

  case class SnapshotTimeout(snapshot: Snapshot)
  case object UnackedResend

  def props(replica: ActorRef): Props = Props(Replicator(replica))

class Replicator(val replica: ActorRef) extends Actor with AkkaHelpers:
  import Replicator.*
  import context.dispatcher
  
  /*
   * The contents of this actor is just a suggestion, you can implement it in any way you like.
   */

  // map from sequence number to pair of sender and request
  var acks = Map.empty[Long, (ActorRef, Replicate)]
  // a sequence of not-yet-sent snapshots (you can disregard this if not implementing batching)
  var pending = Vector.empty[Snapshot]
  val role = "Replicator"
  
  var _seqCounter = 0L
  def nextSeq() =
    val ret = _seqCounter
    _seqCounter += 1
    ret

  override def preStart(): Unit = {
    scheduleOnce(UnackedResend)
  }

  def requestExists(newRequest: Replicate): Boolean = 
    acks.exists{
      case (_, (_, Replicate(key, _, id))) => 
        newRequest.id == id && newRequest.key == key
    }

  def findRequest(msg: Replicate): Option[Long] = 
    acks.find{
      case (_, (_, Replicate(key, _, id))) => 
        msg.id == id && msg.key == key
    }.map(_._1)
  
  // override def logMsg(msg: String): Unit = ()

  def replicateMsg(msg: Replicate): Unit = {
    val seqOpt = findRequest(msg)
    val snapshotOpt = seqOpt.flatMap(seq => pending.find(_.seq == seq))

    snapshotOpt.fold{
      val newSeq = nextSeq()
      val leader = sender
      val newSnapshot = Snapshot(msg.key, msg.valueOption, newSeq)
      acks += newSeq -> (leader, msg)
      pending = pending :+ newSnapshot 
      logMsg(s"[SNAPSHOT] ${replica} ! ${newSnapshot}")
      replica ! newSnapshot
    } { snapshot => 
      // ...
      logMsg(s"[*SNAPSHOT] ${replica} ! ${snapshot}")
      replica ! snapshot
    }
  }
  
  /* TODO Behavior for the Replicator. */
  def receive: Receive =
    case request @ Replicate(key, value, id) =>
      logMsg(s"${role}.Replicate: ${key} -> ${id} = ${value}")
      replicateMsg(request)
    case SnapshotAck(key, seq) if acks contains seq =>
      logMsg(s"${role}.SnapshotAck: ${key} - ${seq}")
      val (leader, request) = acks(seq)
      leader ! Replicated(key, request.id)
      acks -= seq
      pending = pending.filterNot(s => s.seq == seq && s.key == key)
    case UnackedResend if pending.nonEmpty =>
      logMsg("Resending...")
      pending.foreach { snapshot =>
        logMsg(s"${role}.UnackedResend: Resending ${snapshot} ...")
        replica ! snapshot
      }
      scheduleOnce(UnackedResend)
    case _ =>

