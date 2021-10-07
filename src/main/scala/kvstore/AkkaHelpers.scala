package kvstore

import akka.actor.{ OneForOneStrategy, PoisonPill, Props, SupervisorStrategy, Terminated, ActorRef, Actor, actorRef2Scala }
import akka.event.Logging
import scala.concurrent.duration.*

trait AkkaHelpers { this: Actor => 
    val log = Logging(context.system, this)
    import context.dispatcher

    def scheduleOnceDelay: FiniteDuration = 100.milliseconds

    def scheduleOnce[T](duration: FiniteDuration, actor: ActorRef, msg: T): Unit = 
        context.system.scheduler.scheduleOnce(scheduleOnceDelay, actor, msg)

    def scheduleOnce[T](actor: ActorRef, msg: T): Unit = scheduleOnce[T](scheduleOnceDelay, actor, msg)
    def scheduleOnce[T](msg: T): Unit = scheduleOnce[T](scheduleOnceDelay, self, msg)
    def scheduleOnce[T](duration: FiniteDuration, msg: T): Unit = scheduleOnce[T](duration, self, msg)
}