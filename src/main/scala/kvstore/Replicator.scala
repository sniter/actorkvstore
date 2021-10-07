package kvstore

import akka.actor.Props
import akka.actor.Actor
import akka.actor.ActorRef
import akka.actor.actorRef2Scala
import scala.concurrent.duration.*
import java.time.Instant

object Replicator:
  case class Replicate(key: String, valueOption: Option[String], id: Long)
  case class Replicated(key: String, id: Long)
  
  case class Snapshot(key: String, valueOption: Option[String], seq: Long)
  case class SnapshotAck(key: String, seq: Long)

  case class SnapshotTimeout(seq: Long)
  case object UnackedResend

  def props(replica: ActorRef): Props = Props(Replicator(replica))

class Replicator(val replica: ActorRef) extends Actor:
  import Replicator.*
  import context.dispatcher
  
  /*
   * The contents of this actor is just a suggestion, you can implement it in any way you like.
   */

  // map from sequence number to pair of sender and request
  var acks = Map.empty[Long, (ActorRef, Replicate)]
  // a sequence of not-yet-sent snapshots (you can disregard this if not implementing batching)
  var pending = Vector.empty[Snapshot]
  var pending2 = Vector.empty[(Instant, Snapshot)]
  
  var _seqCounter = 0L
  def nextSeq() =
    val ret = _seqCounter
    _seqCounter += 1
    ret

  override def preStart(): Unit = {
    context.system.scheduler.scheduleWithFixedDelay(100.milliseconds, 100.milliseconds, self, UnackedResend)
  }
  
  /* TODO Behavior for the Replicator. */
  def receive: Receive =
    case request @ Replicate(key, value, id) =>
      val newSeq = nextSeq()
      val leader = sender
      val newSnapshot = Snapshot(key, value, newSeq)
      acks = acks + (newSeq -> (leader, request))
      replica ! newSnapshot
      context.system.scheduler.scheduleOnce(100.milliseconds, self, SnapshotTimeout(newSeq))
    case SnapshotAck(key, seq) =>
      val (leader, request) = acks(seq)
      leader ! Replicated(key, request.id)
      acks = acks - seq
    case SnapshotTimeout(seq) if acks.contains(seq) =>
    case UnackedResend if pending.nonEmpty =>
      pending.foreach { snapshot =>
        replica ! snapshot
      }
      pending = pending.empty
    case _ =>

