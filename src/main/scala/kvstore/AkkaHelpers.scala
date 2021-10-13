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

    private val allowedTests = List(
        // "step2-case4",

        // "step4-case1",
        // "step4-case2",
        // "step4-case3",

        // "step5-case1",
        // "step5-case2",
        // "step5-case3",

        "step6-case3",
        // "integration-case1",
        // "integration-case2",
        "integration-case3",
    )

    def logMsg(msg: String): Unit = 
        allowedTests.find(name => self.toString.toLowerCase contains name).foreach{ _ =>
            log.debug(msg)
        }
}