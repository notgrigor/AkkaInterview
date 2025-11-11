import org.apache.pekko.actor.typed._
import org.apache.pekko.actor.typed.scaladsl._

import scala.concurrent.duration._
import scala.util.Random

// ===== Messages =====
final case class Job(id: String, payload: String)

final case class JobDone(id: String)

// ===== Worker =====
object Worker {
  sealed trait Command
  final case class Process(job: Job, replyTo: ActorRef[JobDone]) extends Command

  def apply(name: String): Behavior[Command] =
    Behaviors.setup(ctx => new Worker(ctx, name))
}

class Worker(context: ActorContext[Worker.Command], name: String) 
    extends AbstractBehavior[Worker.Command](context) {
  
  override def onMessage(msg: Worker.Command): Behavior[Worker.Command] = {
    // TODO: Implement worker that processes jobs with random delay (40-120ms)
    // and replies with JobDone
    ???
  }
}

// ===== Balancer =====
object Balancer {
  sealed trait Command
  final case class Submit(job: Job, replyTo: ActorRef[JobDone]) extends Command
  private final case class WrappedDone(done: JobDone) extends Command

  def apply(numWorkers: Int): Behavior[Command] =
    Behaviors.setup(ctx => new Balancer(ctx, numWorkers))
}

class Balancer(context: ActorContext[Balancer.Command], numWorkers: Int) 
    extends AbstractBehavior[Balancer.Command](context) {
  
  // TODO: Implement load balancer that:
  // 1. Creates a pool of worker actors
  // 2. Tracks in-flight jobs (jobId -> original replyTo)
  // 3. Distributes jobs using round-robin strategy
  // 4. Uses messageAdapter to receive JobDone from workers
  // 5. Forwards JobDone back to the original submitter

  override def onMessage(msg: Balancer.Command): Behavior[Balancer.Command] = {
    ???
  }
}

// ===== Demo Driver =====
object Main {
  def main(args: Array[String]): Unit = {
    val system: ActorSystem[Balancer.Command] = ActorSystem(Balancer(4), "lb-simple")

    val client: ActorRef[JobDone] = system.systemActorOf(
      Behaviors.receiveMessage[JobDone] { msg =>
        println(s"DONE ${msg.id}")
        Behaviors.same
      },
      "client"
    )

    for (i <- 0 until 100) {
      system ! Balancer.Submit(Job(s"job-$i", s"payload$i"), client)
    }

    import system.executionContext
    
    system.scheduler.scheduleOnce(
      5.seconds,
      new Runnable { def run(): Unit = system.terminate() }
    )
  }
}
