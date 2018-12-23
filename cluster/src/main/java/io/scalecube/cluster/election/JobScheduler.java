package io.scalecube.cluster.election;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;

public class JobScheduler {

  private AtomicReference<Disposable> disposables = new AtomicReference<>(null);

  private final Consumer job;

  public JobScheduler(final Consumer job) {
    this.job = job;
  }

  /**
   * start running provided job in given interval.
   *
   * @param interval in milli sec to execute the job.
   */
  public void start(Duration interval) {
    if (disposables.get() == null || disposables.get().isDisposed()) {
      disposables.set(Flux.interval(interval).subscribe(job));
    }
  }

  /** stop executing the given job and dispose. */
  public void stop() {
    if (disposables.get() != null && !disposables.get().isDisposed()) {
      disposables.get().dispose();
    }
  }

  /**
   * stop current running job and restart again.
   *
   * @param interval to execute this job in milli.
   */
  public void reset(Duration interval) {
    stop();
    start(interval);
  }
}
