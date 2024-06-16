package io.scalecube.cluster2;

import org.agrona.concurrent.EpochClock;

public class Delay {

  private final EpochClock epochClock;
  private final long defaultDelay;

  private long deadline;

  public Delay(EpochClock epochClock, long defaultDelay) {
    this.epochClock = epochClock;
    this.defaultDelay = defaultDelay;
  }

  public Delay(Delay delay) {
    this.epochClock = delay.epochClock;
    this.defaultDelay = delay.defaultDelay;
    this.deadline = 0;
  }

  public void delay() {
    delay(defaultDelay);
  }

  public void delay(long delay) {
    deadline = epochClock.time() + delay;
  }

  public boolean isOverdue() {
    return epochClock.time() > deadline;
  }

  public boolean isNotOverdue() {
    return epochClock.time() < deadline;
  }
}
