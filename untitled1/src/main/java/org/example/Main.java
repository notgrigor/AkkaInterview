package org.example;

import akka.actor.typed.*;
import akka.actor.typed.javadsl.*;

import java.time.Duration;
import java.util.concurrent.ThreadLocalRandom;

final class Job {
    final String id;
    final String payload;

    Job(String id, String payload) {
        this.id = id;
        this.payload = payload;
    }
}

final class JobDone {
    final String id;

    JobDone(String id) {
        this.id = id;
    }
}

// ===== Worker =====
class Worker {
    interface Command {}

    static final class Process implements Command {
        final Job job;
        final ActorRef<JobDone> replyTo;

        Process(Job job, ActorRef<JobDone> replyTo) {
            this.job = job;
            this.replyTo = replyTo;
        }
    }

    static Behavior<Command> create(String name) {
        return Behaviors.setup(ctx -> Behaviors.receive(Command.class)
            .onMessage(Process.class, msg -> {
                long delay = ThreadLocalRandom.current().nextLong(40, 121);
                try { Thread.sleep(delay); } catch (InterruptedException ignored) {}
                msg.replyTo.tell(new JobDone(msg.job.id));
                return Behaviors.same();
            })
            .build());
    }
}

// ===== Balancer =====
class Balancer {
    interface Command {}

    static final class Submit implements Command {
        final Job job;
        final ActorRef<JobDone> replyTo;

        Submit(Job job, ActorRef<JobDone> replyTo) {
            this.job = job;
            this.replyTo = replyTo;
        }
    }

    static Behavior<Command> create(int workers) {
        return Behaviors.setup(ctx -> {
            // TODO: Implement
            // - Create a pool of workers
            // - Track in-flight jobs (jobId -> replyTo)
            // - Use a strategy (round robin, least busy, random, etc.)
            // - Receive JobDone using ctx.messageAdapter()
            // - Send JobDone back to the original submitter
            return Behaviors.empty(); // replace this
        });
    }
}

// ===== Demo Driver =====
public class Main {
    public static void main(String[] args) {
        ActorSystem<Balancer.Command> system = ActorSystem.create(Balancer.create(4), "lb-simple");

        ActorRef<JobDone> client = system.systemActorOf(
            Behaviors.receiveMessage(msg -> {
                System.out.println("DONE " + msg.id);
                return Behaviors.same();
            }),
            "client"
        );

        for (int i = 0; i < 100; i++) {
            system.tell(new Balancer.Submit(new Job("job-" + i, "payload" + i), client));
        }

        system.scheduler().scheduleOnce(
            Duration.ofSeconds(5),
            system::terminate,
            system.executionContext()
        );
    }
}
