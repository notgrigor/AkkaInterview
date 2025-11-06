package org.example;

import akka.actor.typed.*;
import akka.actor.typed.javadsl.*;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

// ===== Messages =====
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
    interface Command {
    }

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
                    // simulate 40–120 ms of work
                    long delay = ThreadLocalRandom.current().nextLong(40, 121);
                    try {
                        Thread.sleep(delay);
                    } catch (InterruptedException ignored) {
                    }
                    msg.replyTo.tell(new JobDone(msg.job.id));
                    return Behaviors.same();
                })
                .build());
    }
}

// ===== Balancer =====
class Balancer {
    interface Command {
    }

    static final class Submit implements Command {
        final Job job;
        final ActorRef<JobDone> replyTo;

        Submit(Job job, ActorRef<JobDone> replyTo) {
            this.job = job;
            this.replyTo = replyTo;
        }
    }

    private static final class WrappedDone implements Command {
        final JobDone done;

        WrappedDone(JobDone d) {
            this.done = d;
        }
    }

    static Behavior<Command> create(int workers) {
        return Behaviors.setup(ctx -> {
            // spawn worker pool
            List<ActorRef<Worker.Command>> pool = new ArrayList<>();
            for (int i = 0; i < workers; i++) pool.add(ctx.spawn(Worker.create("w" + i), "worker-" + i));

            // adapter to receive JobDone from workers
            ActorRef<JobDone> doneAdapter = ctx.messageAdapter(JobDone.class, WrappedDone::new);

            // Map jobId -> original requester
            Map<String, ActorRef<JobDone>> waiters = new HashMap<>();
            final int[] rr = {0};

            return Behaviors.receive(Command.class)
                    .onMessage(Submit.class, msg -> {
                        // remember who to notify
                        waiters.put(msg.job.id, msg.replyTo);
                        // send to next worker
                        ActorRef<Worker.Command> target = pool.get(rr[0]);
                        rr[0] = (rr[0] + 1) % pool.size();
                        target.tell(new Worker.Process(msg.job, doneAdapter));
                        return Behaviors.same();
                    })
                    .onMessage(WrappedDone.class, w -> {
                        ActorRef<JobDone> client = waiters.remove(w.done.id);
                        if (client != null) client.tell(w.done);
                        return Behaviors.same();
                    })
                    .build();
        });
    }
}

// ===== Demo Driver =====
public class Main {
    public static void main(String[] args) throws Exception {
        ActorSystem<Balancer.Command> system = ActorSystem.create(Balancer.create(4), "lb-simple");

        ActorRef<JobDone> client = system.systemActorOf(
                Behaviors.<JobDone>receive((ctx, msg) -> {
                    System.out.println("DONE " + msg.id);
                    return Behaviors.same();
                }),
                "client", Props.empty()
        );

        // submit 100 jobs quickly
        for (int i = 0; i < 100; i++) {
            system.tell(new Balancer.Submit(new Job("job-" + i, "payload" + i), client));
        }

        // end after a short delay
        system.scheduler().scheduleOnce(
                Duration.ofSeconds(5),
                () -> system.terminate(),
                system.executionContext()
        );
    }
}
