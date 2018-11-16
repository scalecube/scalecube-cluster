package io.scalecube.cluster.leaderelection;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

public class JobScheduler {

  private AtomicReference<ScheduledExecutorService> executor = new AtomicReference<ScheduledExecutorService>(null);

  private final Consumer callable;

  public JobScheduler(final Consumer callable) {
    this.callable = callable;
  }

  public void start(int interval) {
    if (executor.get() == null || executor.get().isShutdown() || executor.get().isTerminated()) {
      this.executor.set(Executors.newScheduledThreadPool(1));
    }

    executor.get().scheduleAtFixedRate(() -> {
      callable.accept(Void.TYPE);
    }, interval, interval, TimeUnit.MILLISECONDS);
  }

  public void stop() {
    
    if (executor.get() != null) {
      executor.get().shutdownNow();
    }
  }

  public void reset(int interval) {
    stop();
    start(interval);
  }
}
