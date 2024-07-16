package io.scalecube.cluster2;

import java.util.function.LongConsumer;
import org.agrona.collections.Long2LongHashMap;
import org.agrona.collections.Long2LongHashMap.EntryIterator;
import org.agrona.concurrent.EpochClock;

public class TimerInvoker {

  private final EpochClock epochClock;

  private final Long2LongHashMap deadlineByTimerId = new Long2LongHashMap(Long.MIN_VALUE);
  private long timerIdCounter;

  public TimerInvoker(EpochClock epochClock) {
    this.epochClock = epochClock;
  }

  public int poll(LongConsumer consumer) {
    int workCount = 0;

    if (deadlineByTimerId.isEmpty()) {
      return workCount;
    }

    final long now = epochClock.time();

    for (final EntryIterator it = deadlineByTimerId.entrySet().iterator(); it.hasNext(); ) {
      it.next();
      final long timerId = it.getLongKey();
      final long deadline = it.getLongValue();
      if (now >= deadline) {
        it.remove();
        if (consumer != null) {
          consumer.accept(timerId);
        }
        workCount++;
      }
    }

    return workCount;
  }

  public long scheduleTimer(long deadline) {
    final long timerId = ++timerIdCounter;
    deadlineByTimerId.put(timerId, deadline);
    return timerId;
  }

  public void cancelTimer(long timerId) {
    deadlineByTimerId.remove(timerId);
  }
}
