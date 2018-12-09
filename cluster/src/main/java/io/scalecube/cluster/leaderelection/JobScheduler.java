package io.scalecube.cluster.leaderelection;

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

  public void start(int millis) {
    if (disposables.get() == null || disposables.get().isDisposed()) {
      disposables.set(Flux.interval(Duration.ofMillis(millis)).subscribe(job));
    }
  }

  public void stop() {
    if (disposables.get() != null && !disposables.get().isDisposed()) {
      disposables.get().dispose();
    }
  }

  public void reset(int millis) {
    stop();
    start(millis);
  }
}
